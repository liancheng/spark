/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.avro

import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

private[avro] class AvroOutputWriterFactory(
    schema: StructType,
    recordName: String,
    recordNamespace: String) extends OutputWriterFactory {

  override def getFileExtension(context: TaskAttemptContext): String = ".avro"

  def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    new AvroOutputWriter(path, context, schema, recordName, recordNamespace)
  }
}
