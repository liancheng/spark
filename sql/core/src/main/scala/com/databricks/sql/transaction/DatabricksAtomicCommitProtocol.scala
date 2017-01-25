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

import org.apache.hadoop.fs.{FileSystem => HadoopFileSystem, _}
import org.apache.hadoop.mapreduce._
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ThreadUtils

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
    val fs = finalPath.getFileSystem(taskContext.getConfiguration)
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
        "com.databricks.sql.enableLogicalDelete", "true").toBoolean) {
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
    val fs = root.getFileSystem(jobContext.getConfiguration)
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
    dirs.tasksupport = DatabricksAtomicCommitProtocol.tasksupport
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
      val fs = f.getFileSystem(taskContext.getConfiguration)
      fs.delete(f, false)
    }
  }
}

object DatabricksAtomicCommitProtocol extends Logging {
  import DatabricksAtomicReadProtocol._

  private val sparkSession = SparkSession.builder.getOrCreate()

  import scala.collection.parallel.ThreadPoolTaskSupport

  private lazy val tasksupport = new ThreadPoolTaskSupport(
    ThreadUtils.newDaemonCachedThreadPool("db-atomic-commit-worker", 100))

  /**
   * Traverses the given directories and cleans up uncommitted or garbage files and markers. A
   * horizon may be specified beyond which we assume pending jobs have failed. Files written by
   * those jobs will be removed as well. Vacuuming will be done in parallel if possible.
   *
   * @return the list of deleted files
   */
  def vacuum(path: Path, horizon: Long): List[Path] = {
    val fs = testingFs.getOrElse(path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration))
    val (dirs, initialFiles) = fs.listStatus(path).partition(_.isDirectory)

    def checkPositive(time: Long): Long = { assert(time > 0); time }
    var deletedPaths: List[Path] = Nil
    def delete(p: Path): Unit = {
      deletedPaths ::= p
      fs.delete(p, false)
    }

    val (state, resolvedFiles) = resolveCommitState(fs, path, initialFiles)

    // remove uncommitted and timed-out file outputs
    for (file <- resolvedFiles) {
      file.getPath.getName match {
        // we wait for a horizon to avoid killing Spark jobs using those files
        case name if state.getDeletionTime(name) > 0 && state.getDeletionTime(name) < horizon =>
          logInfo(s"Garbage collecting ${file.getPath} since it is marked as deleted.")
          delete(file.getPath)

        case name @ FILE_WITH_TXN_ID(txnId) if state.isCommitted(txnId) &&
            !state.isFileCommitted(txnId, name) =>
          logInfo(s"Garbage collecting ${file.getPath} since it was written by a failed task.")
          delete(file.getPath)

        case name @ FILE_WITH_TXN_ID(txnId) if !state.isCommitted(txnId) &&
            checkPositive(state.getStartTime(txnId)) < horizon =>
          logInfo(s"Garbage collecting ${file.getPath} since its job has timed out " +
            s"(${state.getStartTime(txnId)} < $horizon).")
          delete(file.getPath)

        // always safe to delete since the commit marker is present
        case STARTED_MARKER(txnId) if state.isCommitted(txnId) &&
            checkPositive(file.getModificationTime) < horizon =>
          logInfo(s"Garbage collecting start marker ${file.getPath} of committed job.")
          delete(file.getPath)

        case _ =>
      }
    }

    // Queue up stale markers for deletion. We do this by writing out a _committed file that
    // will cause them to be garbage collected in the next cycle.
    var deleteLater: List[Path] = Nil
    for (file <- resolvedFiles) {
      file.getPath.getName match {
        case name @ COMMITTED_MARKER(txnId) if state.getDeletionTime(name) == 0 &&
            checkPositive(file.getModificationTime) < horizon =>
          val startMarker = new Path(file.getPath.getParent, s"_started_$txnId")
          if (fs.exists(startMarker)) {
            delete(startMarker)  // make sure we delete it just in case
          }
          deleteLater ::= file.getPath

        // the data files were deleted above, but we need to delay marker deletion
        case STARTED_MARKER(txnId) if !state.isCommitted(txnId) &&
            checkPositive(file.getModificationTime) < horizon =>
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
      deletedPaths :::= vacuum(d.getPath, horizon)
      if (fs.listStatus(d.getPath).isEmpty) {
        logInfo(s"Garbage collecting empty directory ${d.getPath}")
        delete(d.getPath)
      }
    }

    deletedPaths
  }

  private def newTxnId(): String = math.abs(scala.util.Random.nextLong).toString
}
