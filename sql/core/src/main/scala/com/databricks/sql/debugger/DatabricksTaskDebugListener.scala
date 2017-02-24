/* Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.sql.debugger

import com.databricks.sql.DatabricksSQLConf
import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution


/**
 * A SparkListener that monitors the number of records read and produced by each task.
 * The tasks for which the ratio between these two values exceeds configured limit are terminated.
 *
 * The number of records read is computed as the sum of "internal.metrics.input.recordsRead" and
 * "internal.metrics.shuffle.read.recordsRead" metrics.
 *
 * The number of records produced is the maximum value of "number of output rows" metric. This way
 * we can detect, e.g., cardprod immediately followed by a filter.
 */
class DatabricksTaskDebugListener(
    val sparkContext: SparkContext)
  extends SparkListener
  with Logging {

  // Tasks currently monitored by the listener.
  private[this] val activeTasks: Cache[TaskIdentity, TaskMetrics] =
    CacheBuilder.newBuilder().maximumSize(10000).build[TaskIdentity, TaskMetrics]()

  // Mapping stageIds to corresponding executionIds. This is needed to access the correct
  // SparkSession.conf for each monitored task.
  private[this] val stageIdToExecutionId: Cache[Integer, java.lang.Long] =
    CacheBuilder.newBuilder().maximumSize(1000).build[Integer, java.lang.Long]()

  // Visible for testing.
  def numActiveTasks: Int = activeTasks.asMap().size()

  private[this] case class TaskIdentity(stageId: Int, stageAttemptId: Int, taskId: Long)

  /**
   * Helper class for managing the metric updates already received from a given task.
   * Stores only the highest received values of the number of read/generated/produced records.
   */
  private[this] class TaskMetrics(val taskIdent: TaskIdentity,
      val launchTime: Long,
      val executionId: Long) {

    var cancelRequestIssued = false

    /**
     * Find the maximum number of records read (or generated) and records outputted in the provided
     * metric updates.
     */
    def processMetricUpdates(accumInfos: Seq[AccumulableInfo]): Unit = {
      var recordsIn = 0L
      var recordsOut = 0L

      accumInfos.foreach { accumInfo =>
        accumInfo.update match {
          case Some(n: Number) =>
            val metricName = accumInfo.name.toString()
            val metricVal = n.longValue()
            if (metricName.contains("recordsRead")) {
              recordsIn += metricVal
            } else if (metricName.contains("output rows") && recordsOut < metricVal) {
              recordsOut = metricVal
            }
          case _ => // Ignore other metrics.
        }
      }

      checkOutputRatio(recordsIn, recordsOut)
    }

    /**
     * Compare running time and output ratio and with the configured limits.
     * If needed, request task cancellation.
     */
    private def checkOutputRatio(recordsIn: Long, recordsOut: Long): Unit = {
      val outputRatio = if (recordsIn > 0) recordsOut / recordsIn else 0

      val queryExecution = SQLExecution.getQueryExecution(executionId)
      if (!cancelRequestIssued || launchTime > 0 || queryExecution != null) {
        val minRunningTimeSec = queryExecution.sparkSession.sessionState.conf.getConf(
            DatabricksSQLConf.TASK_KILLER_MIN_TIME)
        val minOutputRows = queryExecution.sparkSession.sessionState.conf.getConf(
            DatabricksSQLConf.TASK_KILLER_MIN_OUTPUT_ROWS)
        val outputRatioKillThreshold = queryExecution.sparkSession.sessionState.conf.getConf(
            DatabricksSQLConf.TASK_KILLER_OUTPUT_RATIO_THRESHOLD)
        val runningTimeSec = (System.currentTimeMillis() - launchTime) / 1000

        if (runningTimeSec > minRunningTimeSec && recordsOut >= minOutputRows &&
            outputRatioKillThreshold > 0 && outputRatio > outputRatioKillThreshold) {
          val errorMsgTemplate = queryExecution.sparkSession.sessionState.conf.getConf(
              DatabricksSQLConf.TASK_KILLER_ERROR_MESSAGE)
          terminateTask(outputRatio, outputRatioKillThreshold, errorMsgTemplate)
        }
      }
    }

    // Request cancellation of the job to which this task belongs.
    private def terminateTask(
        outputRatio: Long,
        outputRatioKillThreshold: Long,
        errorMsgTemplate: String): Unit = {

      val subs = Seq("taskId" -> taskIdent.taskId, "stageId" -> taskIdent.stageId,
          "outputRatio" -> outputRatio, "outputRatioKillThreshold" -> outputRatioKillThreshold)
      val errorMsg = subs.foldLeft(errorMsgTemplate) { (msg: String, sub: (String, Any)) =>
        msg.replaceAll("\\$\\{" + sub._1 + "\\}", sub._2.toString)
      }
      logWarning(errorMsg)
      sparkContext.cancelStage(taskIdent.stageId, errorMsg)
      cancelRequestIssued = true
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdString = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString != null) {
      val executionId = executionIdString.toLong
      val stageIds = jobStart.stageIds
      for (stageId <- stageIds) {
        stageIdToExecutionId.put(stageId, executionId)
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageIdToExecutionId.invalidate(stageCompleted.stageInfo.stageId)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskIdent = TaskIdentity(taskStart.stageId, taskStart.stageAttemptId,
        taskStart.taskInfo.taskId)

    val executionId = stageIdToExecutionId.getIfPresent(taskStart.stageId)
    if (executionId != null) {
      val taskMetrics = new TaskMetrics(taskIdent, taskStart.taskInfo.launchTime, executionId)
      activeTasks.put(taskIdent, taskMetrics)
    }
  }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    executorMetricsUpdate.accumUpdates.foreach {
      case (taskId, stageId, stageAttemptId, accumInfos) =>
        val taskMetrics = activeTasks.getIfPresent(TaskIdentity(stageId, stageAttemptId, taskId))
        if(taskMetrics != null) {
          taskMetrics.processMetricUpdates(accumInfos)
        }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskIdent = TaskIdentity(taskEnd.stageId, taskEnd.stageAttemptId, taskEnd.taskInfo.taskId)
    activeTasks.invalidate(taskIdent)
  }
}
