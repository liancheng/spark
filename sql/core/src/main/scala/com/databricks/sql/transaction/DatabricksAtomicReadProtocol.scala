/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.transaction

import java.io.{File, FileNotFoundException, InputStream, InputStreamReader, IOException, OutputStream}
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce._
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * Read-side support for DatabricksAtomicCommitProtocol.
 */
object DatabricksAtomicReadProtocol extends Logging {
  type TxnId = String

  import scala.collection.parallel.ThreadPoolTaskSupport

  lazy val tasksupport = new ThreadPoolTaskSupport(
    ThreadUtils.newDaemonCachedThreadPool("db-atomic-commit-worker", 100))

  val STARTED_MARKER = "_started_(.*)".r
  val COMMITTED_MARKER = "_committed_(.*)".r
  val FILE_WITH_TXN_ID = "[^_].*-tid-([a-z0-9]+)-.*".r

  private implicit val formats = Serialization.formats(NoTypeHints)

  // Visible for testing.
  private[spark] var testingFs: Option[FileSystem] = None

  // Visible for testing.
  private[spark] var clock: Clock = new SystemClock

  /**
   * Given a directory listing, filters out files that are uncommitted. A file is considered
   * committed if it is named in a `_committed-$txnId` marker, OR if there is no corresponding
   * `_committed-$txnId` or `_started-$txnId` marker for the file.
   *
   * @return the filtered list of files
   */
  def filterDirectoryListing(
      fs: FileSystem, dir: Path, initialFiles: Seq[FileStatus]): Seq[FileStatus] = {
    // we use SparkEnv for this escape-hatch flag since this may be called on executors
    if (!SparkEnv.get.conf.getBoolean("spark.databricks.sql.enableFilterUncommitted", true)) {
      return initialFiles
    }

    val (state, resolvedFiles) = resolveCommitState(testingFs.getOrElse(fs), dir, initialFiles)
    resolvedFiles.filter { f =>
      val name = f.getPath.getName
      name match {
        case _ if state.getDeletionTime(name) > 0 =>
          logDebug(s"Ignoring ${f.getPath} since it is marked as deleted.")
          false
        case FILE_WITH_TXN_ID(txnId) if !state.isFileCommitted(txnId, name) =>
          logDebug(s"Ignoring ${f.getPath} since it is not marked as committed.")
          false
        case _ =>
          true
      }
    }
  }

  /**
   * Holds the parsed commit state of files local to a single directory.
   *
   * @param lastModified max modification time of files in this dir
   * @param trackedFiles list of all files with txn ids
   * @param startMarkers set of start markers found, and their creation times
   * @param commitMarkers set of commit markers found, and their added files
   * @param corruptCommitMarkers set of commit markers we could not read
   * @param deletedFiles set of files marked as deleted by committed transactions
   */
  private[transaction] class CommitState(
      val lastModified: Long,
      trackedFiles: Map[String, TxnId],
      startMarkers: Map[TxnId, Long],
      commitMarkers: Map[TxnId, Set[String]],
      corruptCommitMarkers: Set[TxnId],
      deletedFiles: Map[String, Long]) {

    // The set of transaction ids from which we might be missing start markers.
    val missingMarkers: Set[TxnId] = {
      trackedFiles.values.toSet.diff(
        (startMarkers.keys ++ commitMarkers.keys ++ corruptCommitMarkers).toSet)
    }

    // The set of files which are should be present but are missing.
    val missingDataFiles: Set[String] = {
      commitMarkers.values.flatten.toSet -- trackedFiles.keys -- deletedFiles.keys
    }

    /**
     * @return whether the given transaction has committed (untracked txns are also committed).
     */
    def isCommitted(txnId: TxnId): Boolean = {
      commitMarkers.contains(txnId) || !startMarkers.contains(txnId)
    }

    /**
     * @return whether the given file is committed (untracked files are also considered committed).
     */
    def isFileCommitted(txnId: TxnId, filename: String): Boolean = {
      isCommitted(txnId) &&
        (!commitMarkers.contains(txnId) || commitMarkers(txnId).contains(filename))
    }

    /**
     * @return the approximate start timestamp of the pending transaction, otherwise throws.
     */
    def getStartTime(txnId: TxnId): Long = startMarkers(txnId)

    /**
     * @return the deletion time of the file, or zero if it is not marked as deleted.
     */
    def getDeletionTime(filename: String): Long = deletedFiles.getOrElse(filename, 0L)
  }

  /**
   * Given the list of files in a directory, parses and returns the per-file commit state. This
   * may require addition IOs to resolve apparent write reordering and read commit file contents.
   *
   * Details on apparent write reordering:
   *
   *   S3 will (soon) provide consistent LIST-after-PUT for single keys. This does not mean that
   *   readers will observe writes in order, however, due to the lack of snapshot isolation within
   *   a single LIST operation.
   *
   *   Write order visibility is a problem if a start marker PUT get re-ordered after a data file
   *   write from the reader perspective. To work around this issue, we list the directory again
   *   if a start marker is suspected to be missing.
   *
   *   The same issue can occur with data file writes re-ordered after commit marker creation. In
   *   this situation we also must re-list if data files are suspected to be missing.
   */
  private[transaction] def resolveCommitState(
      fs: FileSystem,
      dir: Path,
      initialFiles: Seq[FileStatus]): (CommitState, Seq[FileStatus]) = {
    val state = resolveCommitState0(fs, dir, initialFiles)

    // Optimization: can assume the list request was atomic if the files have not changed recently.
    val horizonMillis = SparkEnv.get.conf.getLong(
      "spark.databricks.sql.writeReorderingHorizonMillis", 5 * 60 * 1000)

    if ((state.missingMarkers.nonEmpty || state.missingDataFiles.nonEmpty) &&
          state.lastModified > clock.getTimeMillis - horizonMillis) {
      logInfo("Repeating list request since some files are suspected to be missing.")
      val newlyCommitted = mutable.Set[TxnId]()
      val extraStatuses = fs.listStatus(dir).filter { f =>
        f.isFile && (f.getPath.getName match {
          case COMMITTED_MARKER(txnId) if
              f.getLen > 0 && state.missingMarkers.contains(txnId) =>
            // We choose to drop all files from transactions that committed during the re-list.
            // Otherwise, we'd have to do another round of re-listing to resolve ordering issues.
            newlyCommitted += txnId
            false
          case STARTED_MARKER(txnId) if state.missingMarkers.contains(txnId) => true
          case name @ FILE_WITH_TXN_ID(_) if state.missingDataFiles.contains(name) => true
          case _ => false
        })
      }

      // log a debug message if data files are still missing
      state.missingDataFiles.diff(extraStatuses.map(_.getPath.getName).toSet) match {
        case missing if missing.nonEmpty =>
          logWarning(
            "These files are still missing after a re-list (maybe manually deleted): " + missing)
        case _ =>
      }

      if (extraStatuses.nonEmpty || newlyCommitted.nonEmpty) {
        if (extraStatuses.nonEmpty) {
          logWarning(
            "Found these missing files on the second read: " + extraStatuses.map(_.getPath).toSeq)
        }
        if (newlyCommitted.nonEmpty) {
          logWarning(
            "Found these newly committed jobs on the second read: " + newlyCommitted)
        }
        val newFiles = (initialFiles ++ extraStatuses).filter { f =>
          f.getPath.getName match {
            case name @ FILE_WITH_TXN_ID(txnId) if newlyCommitted.contains(txnId) => false
            case _ => true
          }
        }
        (resolveCommitState0(fs, dir, newFiles), newFiles)
      } else {
        (state, initialFiles)
      }
    } else {
      logDebug("List request was not repeated since " + state.missingMarkers.nonEmpty + " " +
        state.missingDataFiles.nonEmpty + " " + state.lastModified + " " + clock.getTimeMillis +
        " " + horizonMillis)
      (state, initialFiles)
    }
  }

  private def resolveCommitState0(
      fs: FileSystem,
      dir: Path,
      filesAndMarkers: Seq[FileStatus]): CommitState = {

    var lastModified: Long = 0L
    val trackedFiles = mutable.Map[String, TxnId]()
    val startMarkers = mutable.Map[TxnId, Long]()
    val commitMarkers = mutable.Map[TxnId, Set[String]]()
    val corruptCommitMarkers = mutable.Set[TxnId]()
    val deletedFiles = mutable.Map[String, Long]()

    // Retrieve all file contents in parallel to hide the IO latency.
    val fileContents: Map[TxnId, Try[(Seq[String], Seq[String])]] = {
      val pcol = filesAndMarkers.par
      pcol.tasksupport = tasksupport
      pcol.flatMap { stat =>
        stat.getPath.getName match {
          // We ignore zero-length commit markers (this is a commonly observed symptom of DBFS
          // cancellation bugs in practice).
          case COMMITTED_MARKER(txnId) if stat.getLen > 0 =>
            val commitFile = new Path(dir, "_committed_" + txnId)
            val result = Try(deserializeFileChanges(fs.open(commitFile)))
            Some((txnId, result))
          case _ =>
            None
        }
      }.toList.toMap
    }

    filesAndMarkers.foreach { stat =>
      if (stat.getModificationTime > lastModified) {
        lastModified = stat.getModificationTime
      }
      stat.getPath.getName match {
        // We ignore zero-length commit markers (this is a commonly observed symptom of DBFS
        // cancellation bugs in practice).
        case COMMITTED_MARKER(txnId) if stat.getLen > 0 =>
          try {
            val (filesAdded, filesRemoved) = fileContents(txnId).get
            filesRemoved.foreach { file =>
              assert(stat.getModificationTime > 0)
              deletedFiles(file) = stat.getModificationTime
            }
            commitMarkers(txnId) = filesAdded.toSet
          } catch {
            case e: FileNotFoundException =>
              logWarning("Job commit marker disappeared before we could read it: " + stat)
              corruptCommitMarkers.add(txnId)

            case NonFatal(e) =>
              // we use SparkEnv for this escape-hatch flag since this may be called on executors
              if (SparkEnv.get.conf.getBoolean(
                  "spark.databricks.sql.ignoreCorruptCommitMarkers", false)) {
                logWarning("Failed to read job commit marker: " + stat, e)
                corruptCommitMarkers.add(txnId)
              } else {
                throw new IOException("Failed to read job commit marker: " + stat, e)
              }
          }

        case STARTED_MARKER(txnId) =>
          assert(stat.getModificationTime > 0)
          startMarkers(txnId) = stat.getModificationTime

        case FILE_WITH_TXN_ID(txnId) =>
          trackedFiles(stat.getPath.getName) = txnId

        case _ =>
      }
    }

    new CommitState(
      lastModified,
      trackedFiles.toMap,
      startMarkers.toMap,
      commitMarkers.toMap,
      corruptCommitMarkers.toSet,
      deletedFiles.toMap)
  }

  def serializeFileChanges(
      filesAdded: Seq[String], filesRemoved: Seq[String], out: OutputStream): Unit = {
    val changes = Map("added" -> filesAdded, "removed" -> filesRemoved)
    logDebug("Writing out file changes: " + changes)
    Serialization.write(changes, out)
  }

  def deserializeFileChanges(in: InputStream): (Seq[String], Seq[String]) = {
    val reader = new InputStreamReader(in, StandardCharsets.UTF_8)
    try {
      val changes = Serialization.read[Map[String, Any]](reader)
      (changes("added").asInstanceOf[Seq[String]], changes("removed").asInstanceOf[Seq[String]])
    } finally {
      reader.close()
    }
  }
}
