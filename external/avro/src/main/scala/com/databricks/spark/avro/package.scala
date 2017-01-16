/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object avro {
  /**
   * Adds a method, `avro`, to DataFrameWriter that allows you to write avro files using
   * the DataFileWriter
   */
  implicit class AvroDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def avro: String => Unit = writer.format("com.databricks.spark.avro").save
  }

  /**
   * Adds a method, `avro`, to DataFrameReader that allows you to read avro files using
   * the DataFileReade
   */
  implicit class AvroDataFrameReader(reader: DataFrameReader) {
    def avro: String => DataFrame = reader.format("com.databricks.spark.avro").load
  }
}
