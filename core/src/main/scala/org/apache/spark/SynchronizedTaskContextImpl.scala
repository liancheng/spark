/*
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskCompletionListenerException, TaskFailureListener}

/**
 * A synchronized [[TaskContext]] implementation. This class will delegate most calls to a
 * parent context, an exception to this rule are the completion and failure listeners. These are
 * managed by the context itself.
 *
 * The context will lock it self for operations with potential side-effects. This is part of the
 * contract, and this can be used to synchronize operations involving multiple threads in a
 * single task.
 *
 * Please note that some of the objects exposed by a [[TaskContext]] are inherently
 * non-threadsafe, and cannot be used in thread safe way unless all access is always
 * synchronized using the same lock (preferably the context).
 */
class SynchronizedTaskContextImpl(val parent: TaskContext) extends TaskContext with Logging {
  parent.addTaskCompletionListener { _ =>
    invokeListeners(onCompleteCallbacks, "TaskCompletionListener", None) { listener =>
      listener.onTaskCompletion(this)
    }
  }

  parent.addTaskFailureListener { (_, error) =>
    invokeListeners(onFailureCallbacks, "TaskFailureListener", Option(error)) { listener =>
      listener.onTaskFailure(this, error)
    }
  }

  private def invokeListeners[T](
      listeners: Seq[T],
      name: String,
      error: Option[Throwable])(
      callback: T => Unit): Unit = {
    synchronized {
      // Code copied from TaskContextImpl
      val errorMsgs = new ArrayBuffer[String](2)
      // Process failure callbacks in the reverse order of registration
      listeners.reverse.foreach { listener =>
        try {
          callback(listener)
        } catch {
          case e: Throwable =>
            errorMsgs += e.getMessage
            logError(s"Error in $name", e)
        }
      }
      if (errorMsgs.nonEmpty) {
        throw new TaskCompletionListenerException(errorMsgs, error)
      }
    }
  }

  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  @transient private val onFailureCallbacks = new ArrayBuffer[TaskFailureListener]

  override def stageId(): Int = parent.stageId()

  override def partitionId(): Int = parent.partitionId()

  override def attemptNumber(): Int = parent.attemptNumber()

  override def taskAttemptId(): Long = parent.taskAttemptId()

  override def isCompleted(): Boolean = parent.isCompleted()

  override def isInterrupted(): Boolean = parent.isInterrupted()

  override def isRunningLocally(): Boolean = parent.isRunningLocally()

  override def getLocalProperty(key: String): String = parent.getLocalProperty(key)

  override def taskMetrics(): TaskMetrics = parent.taskMetrics()

  override def getMetricsSources(sourceName: String): Seq[Source] = synchronized {
    parent.getMetricsSources(sourceName)
  }

  override private[spark] def taskMemoryManager(): TaskMemoryManager = parent.taskMemoryManager()

  override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = synchronized {
    parent.registerAccumulator(a)
  }

  override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
    synchronized {
      onCompleteCallbacks += listener
    }
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): TaskContext = {
    synchronized {
      onFailureCallbacks += listener
    }
    this
  }
}
