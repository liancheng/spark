/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.transaction

import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem => HadoopFileSystem, _}
import org.apache.hadoop.mapreduce._
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession

/**
 * File commit protocol optimized for cloud storage. Files are written directly to their final
 * locations. Their commit status is determined by the presence of specially named marker files.
 *
 * Job commit proceeds as follows:
 *
 *  1) When tasks request a file to write, we create a `_started_$txnId` marker in the output
 *     directory. The output files, which have $txnId embedded in their name, are hidden from
 *     readers while the start marker is present.
 *  2) We commit the job by creating a new `_committed_$txnId` marker that contains a list of
 *     files added and removed in that directory.
 *
 *  Note that this is only atomic per-directory, and that we only provide snapshot isolation and
 *  not serializability.
 */
class DatabricksAtomicCommitProtocol(jobId: String, path: String)
  extends FileCommitProtocol with Serializable with Logging {

  import FileCommitProtocol._
  import DatabricksAtomicReadProtocol._
  import DatabricksAtomicCommitProtocol._

  // Globally unique alphanumeric string. We decouple this from jobId for possible future use.
  private val txnId: TxnId = newTxnId()

  // The list of files staged by this committer. These are collected to the driver on task commit.
  private val stagedFiles = mutable.Set[String]()

  // The list of files staged for deletion by the driver.
  @transient private val stagedDeletions = mutable.Set[Path]()

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    if (dir.isDefined) {
      newTaskTempFileAbsPath(taskContext, new Path(path, dir.get).toString, ext)
    } else {
      newTaskTempFileAbsPath(taskContext, new Path(path).toString, ext)
    }
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    val filename = getFilename(taskContext, ext)
    val finalPath = new Path(absoluteDir, filename)
    val fs = testingFs.getOrElse(finalPath.getFileSystem(taskContext.getConfiguration))
    val startMarker = new Path(finalPath.getParent, new Path(s"_started_$txnId"))
    if (!fs.exists(startMarker)) {
      fs.create(startMarker, true).close()
      logDebug("Created start marker: " + startMarker)
    }
    stagedFiles += finalPath.toString
    finalPath.toString
  }

  override def deleteWithJob(_fs: HadoopFileSystem, path: Path, recursive: Boolean): Boolean = {
    val fs = testingFs.getOrElse(_fs)
    val sparkSession = SparkSession.getActiveSession.get
    if (!sparkSession.sqlContext.getConf(
        "spark.databricks.sql.enableLogicalDelete", "true").toBoolean) {
      return super.deleteWithJob(fs, path, recursive)
    }
    if (recursive && fs.getFileStatus(path).isFile) {
      // In this case Spark is attempting to delete a file to make room for a directory.
      // We cannot stage this sort of deletion, so just perform it immediately.
      logWarning(s"Deleting $path immediately since it is a file not directory.")
      return super.deleteWithJob(fs, path, true)
    }
    if (recursive) {
      val (dirs, initialFiles) = fs.listStatus(path).partition(_.isDirectory)
      val resolvedFiles = filterDirectoryListing(fs, path, initialFiles)
      stagedDeletions ++= resolvedFiles.map(_.getPath).filter { path =>
        path.getName match {
          // Don't allow our metadata markers to be deleted with this API. That can result in
          // unexpected results if e.g. a start marker is deleted in the middle of a job.
          case STARTED_MARKER(_) | COMMITTED_MARKER(_) => false
          case _ => true
        }
      }.toList
      dirs.foreach { dir =>
        deleteWithJob(fs, dir.getPath, true)
      }
    } else {
      if (fs.getFileStatus(path).isDirectory) {
        throw new IOException(s"Cannot delete directory $path unless recursive=true.")
      }
      stagedDeletions += path
    }
    true
  }

  private def getFilename(taskContext: TaskAttemptContext, ext: String): String = {
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId

    // Include the job and task attempt ids so that file writes never collide.
    val taskAttemptId = taskContext.getTaskAttemptID.getId

    // e.g. part-00001-tid-177723428-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.gz.parquet
    f"part-$split%05d-tid-$txnId-$jobId-$taskAttemptId$ext"
  }

  override def setupJob(jobContext: JobContext): Unit = {
    val root = new Path(path)
    root.getFileSystem(jobContext.getConfiguration).mkdirs(root)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    logInfo("Committing job " + jobId)
    val root = new Path(path)
    val fs = testingFs.getOrElse(root.getFileSystem(jobContext.getConfiguration))
    def qualify(path: Path): Path = path.makeQualified(fs.getUri, fs.getWorkingDirectory)

    // Collects start markers and staged task files.
    taskCommits.foreach { t =>
      val task = t.obj.asInstanceOf[this.type]
      stagedFiles ++= task.stagedFiles
    }

    val addedByDir = stagedFiles.toSeq.map(new Path(_)).map(qualify)
      .groupBy(_.getParent).map(kv => (kv._1, kv._2.map(_.getName.toString)))

    val removedByDir = stagedDeletions.toSeq.map(qualify)
      .groupBy(_.getParent).map(kv => (kv._1, kv._2.map(_.getName.toString)))

    // Commit each updated directory in parallel.
    val dirs = (addedByDir.keys ++ removedByDir.keys).toSet.par
    dirs.tasksupport = tasksupport
    dirs.foreach { dir =>
      val commitMarker = new Path(dir, s"_committed_$txnId")
      val output = fs.create(commitMarker)
      try {
        serializeFileChanges(
          addedByDir.getOrElse(dir, Nil), removedByDir.getOrElse(dir, Nil), output)
      } finally {
        output.close()
      }
      // We don't delete the start marker here since from a correctness perspective, it is
      // possible a concurrent reader sees neither the start nor end marker even with a re-list
    }
    logInfo("Job commit completed for " + jobId)

    // Optional auto-vacuum.
    val sparkSession = SparkSession.getActiveSession.get
    if (sparkSession.sqlContext.getConf(
        "spark.databricks.sql.autoVacuumOnCommit", "true").toBoolean) {
      logInfo("Auto-vacuuming directories updated by " + jobId)
      try {
        vacuum(sparkSession, dirs.seq.toSeq, None)
      } catch {
        case NonFatal(e) =>
          logError("Auto-vacuum failed with error", e)
      }
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    /* no-op */
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    /* no-op */
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    new TaskCommitMessage(this)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    // We must leave the start markers since other stray tasks may be writing to this same
    // directory, and we need to ensure their files stay hidden.
    stagedFiles.map(new Path(_)).foreach { f =>
      val fs = testingFs.getOrElse(f.getFileSystem(taskContext.getConfiguration))
      fs.delete(f, false)
    }
  }
}

object DatabricksAtomicCommitProtocol extends Logging {
  import DatabricksAtomicReadProtocol._

  /**
   * Traverses the given directories and cleans up uncommitted or garbage files and markers. A
   * horizon may be specified beyond which we assume pending jobs have failed. Files written by
   * those jobs will be removed as well. Vacuuming will be done in parallel if possible.
   *
   * @return the list of deleted files
   */
  def vacuum(
      sparkSession: SparkSession, paths: Seq[Path], horizonHours: Option[Double]): List[Path] = {
    val now = clock.getTimeMillis
    val defaultDataHorizonHours = sparkSession.sqlContext.getConf(
      "spark.databricks.sql.vacuum.dataHorizonHours", "48.0").toDouble
    val dataHorizonHours = horizonHours.getOrElse(defaultDataHorizonHours)
    val dataHorizon = now - (dataHorizonHours * 60 * 60 * 1000).toLong

    // Vacuum will start removing commit markers after this time has passed, so this should be
    // greater than the max amount of time we think a zombie executor can hang around and write
    // output after the job has finished. TODO(ekl) move to spark edge conf
    val metadataHorizonHours = sparkSession.sqlContext.getConf(
      "spark.databricks.sql.vacuum.metadataHorizonHours", "0.5").toDouble
    val metadataHorizon = math.max(
      dataHorizon,
      now - metadataHorizonHours * 60 * 60 * 1000).toLong

    logInfo(
      s"Started VACUUM on $paths with data horizon $dataHorizon (now - $dataHorizonHours hours), " +
      s"metadata horizon $metadataHorizon (now - $metadataHorizonHours hours)")

    if (paths.length == 1) {
      vacuum0(paths(0), dataHorizon, metadataHorizon, sparkSession.sparkContext.hadoopConfiguration)
    } else {
      val pseq = paths.par
      pseq.tasksupport = tasksupport
      pseq.map { p =>
        vacuum0(p, dataHorizon, metadataHorizon, sparkSession.sparkContext.hadoopConfiguration)
      }.reduce(_ ++ _)
    }
  }

  private[transaction] def vacuum0(
      path: Path, dataHorizon: Long, metadataHorizon: Long, conf: Configuration): List[Path] = {
    val fs = testingFs.getOrElse(path.getFileSystem(conf))
    val (dirs, initialFiles) = fs.listStatus(path).partition(_.isDirectory)

    def checkPositive(time: Long): Long = { assert(time > 0); time }
    var deletedPaths: List[Path] = Nil
    def delete(p: Path): Unit = {
      deletedPaths ::= p
      fs.delete(p, false)
    }

    val (state, resolvedFiles) = resolveCommitState(fs, path, initialFiles)

    def canDelete(name: String): Boolean = {
      val deletionTime = state.getDeletionTime(name)
      (isMetadataFile(name) && deletionTime <= metadataHorizon) ||
        (!isMetadataFile(name) && deletionTime <= dataHorizon)
    }

    for (file <- resolvedFiles) {
      file.getPath.getName match {
        // Files marked for deletion, either by an overwrite transaction or previous VACUUM.
        case name if state.isDeleted(name) && canDelete(name) =>
          logInfo(s"Garbage collecting file ${file.getPath} marked for deletion.")
          delete(file.getPath)

        // Data files from failed tasks. They can be removed immediately since it is guaranteed
        // no one will read them.
        case name @ FILE_WITH_TXN_ID(txnId) if state.isCommitted(txnId) &&
            !state.isFileCommitted(txnId, name) =>
          logInfo(s"Garbage collecting ${file.getPath} since it was written by a failed task.")
          delete(file.getPath)

        // Data files from timed out transactions can be removed, since we assume the job has
        // aborted by this time.
        case name @ FILE_WITH_TXN_ID(txnId) if !state.isCommitted(txnId) &&
            checkPositive(state.getStartTime(txnId)) <= dataHorizon =>
          logWarning(s"Garbage collecting ${file.getPath} since its job has timed out " +
            s"(${state.getStartTime(txnId)} <= $dataHorizon).")
          delete(file.getPath)

        // Start markers from committed transactions can be removed after a short grace period,
        // since if we see the commit marker, the start marker is irrelevant.
        case STARTED_MARKER(txnId) if state.isCommitted(txnId) &&
            checkPositive(state.getCommitTime(txnId)) <= metadataHorizon =>
          logInfo(s"Garbage collecting start marker ${file.getPath} of committed job.")
          delete(file.getPath)

        case _ =>
      }
    }

    // There are some files we'd like to delete, but cannot safely do in this pass since we must
    // ensure the deletes here are observed only after the deletes above are observed. To work
    // around this we queue them up for deletion by writing out a _committed file that will cause
    // them to be garbage collected in the next call to vacuum.
    var deleteLater: List[Path] = Nil
    for (file <- resolvedFiles) {
      file.getPath.getName match {
        // Commit markers from committed transactions.
        case name @ COMMITTED_MARKER(txnId) if !state.isDeleted(name) =>
          // When a commit does not remove any data files, we can delete it earlier. Otherwise we
          // have to wait for the data horizon to pass, since those files must be purged first.
          val dataFilesAlreadyPurged = checkPositive(file.getModificationTime) <= dataHorizon
          if (dataFilesAlreadyPurged || (!state.removesDataFiles(txnId) &&
              checkPositive(file.getModificationTime) <= metadataHorizon)) {
            // Corresponding start marker and data files are guaranteed to be purged already.
            deleteLater ::= file.getPath
          }

        // Start markers from timed out transactions.
        case STARTED_MARKER(txnId) if !state.isCommitted(txnId) &&
            checkPositive(file.getModificationTime) <= dataHorizon =>
          deleteLater ::= file.getPath

        case _ =>
      }
    }

    if (deleteLater.nonEmpty) {
      val vacuumCommitMarker = new Path(path, "_committed_vacuum" + newTxnId())
      val output = fs.create(vacuumCommitMarker)
      deleteLater ::= vacuumCommitMarker  // it's self-deleting!
      try {
        serializeFileChanges(Nil, deleteLater.map(_.getName), output)
      } finally {
        output.close()
      }
    }

    // recurse
    for (d <- dirs) {
      deletedPaths :::= vacuum0(d.getPath, dataHorizon, metadataHorizon, conf)
      if (fs.listStatus(d.getPath).isEmpty) {
        logInfo(s"Garbage collecting empty directory ${d.getPath}")
        delete(d.getPath)
      }
    }

    deletedPaths
  }

  private def newTxnId(): String = math.abs(scala.util.Random.nextLong).toString
}
