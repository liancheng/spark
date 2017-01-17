/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import java.io.Closeable

import org.apache.hadoop.mapreduce.RecordReader

/**
 * An adaptor from a Hadoop [[RecordReader]] to an [[Iterator]] over the values returned.
 *
 * This is copied from Apache Spark and is inlined here to avoid depending on Spark internals
 * in this external library.
 */
private[redshift] class RecordReaderIterator[T](
  private[this] var rowReader: RecordReader[_, T]) extends Iterator[T] with Closeable {
  private[this] var havePair = false
  private[this] var finished = false

  override def hasNext: Boolean = {
    if (!finished && !havePair) {
      finished = !rowReader.nextKeyValue
      if (finished) {
        // Close and release the reader here; close() will also be called when the task
        // completes, but for tasks that read from many files, it helps to release the
        // resources early.
        close()
      }
      havePair = !finished
    }
    !finished
  }

  override def next(): T = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    havePair = false
    rowReader.getCurrentValue
  }

  override def close(): Unit = {
    if (rowReader != null) {
      try {
        // Close the reader and release it. Note: it's very important that we don't close the
        // reader more than once, since that exposes us to MAPREDUCE-5918 when running against
        // older Hadoop 2.x releases. That bug can lead to non-deterministic corruption issues
        // when reading compressed input.
        rowReader.close()
      } finally {
        rowReader = null
      }
    }
  }
}
