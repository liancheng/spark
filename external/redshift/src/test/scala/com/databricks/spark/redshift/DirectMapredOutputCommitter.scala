/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._

class DirectMapredOutputCommitter extends OutputCommitter {
  override def setupJob(jobContext: JobContext): Unit = { }

  override def setupTask(taskContext: TaskAttemptContext): Unit = { }

  override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = {
    // We return true here to guard against implementations that do not handle false correctly.
    // The meaning of returning false is not entirely clear, so it's possible to be interpreted
    // as an error. Returning true just means that commitTask() will be called, which is a no-op.
    true
  }

  override def commitTask(taskContext: TaskAttemptContext): Unit = { }

  override def abortTask(taskContext: TaskAttemptContext): Unit = { }

  /**
   * Creates a _SUCCESS file to indicate the entire job was successful.
   * This mimics the behavior of FileOutputCommitter, reusing the same file name and conf option.
   */
  override def commitJob(context: JobContext): Unit = {
    val conf = context.getJobConf
    if (shouldCreateSuccessFile(conf)) {
      val outputPath = FileOutputFormat.getOutputPath(conf)
      if (outputPath != null) {
        val fileSys = outputPath.getFileSystem(conf)
        val filePath = new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME)
        fileSys.create(filePath).close()
      }
    }
  }

  /** By default, we do create the _SUCCESS file, but we allow it to be turned off. */
  private def shouldCreateSuccessFile(conf: JobConf): Boolean = {
    conf.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)
  }
}
