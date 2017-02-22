/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import java.sql.SQLException

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * End-to-end tests of functionality which involves writing to Redshift via the connector.
 */
abstract class BaseRedshiftWriteSuite extends IntegrationSuiteBase {

  protected val tempformat: String

  override protected def write(df: DataFrame): DataFrameWriter[Row] =
    super.write(df).option("tempformat", tempformat)

  test("roundtrip save and load") {
    // This test can be simplified once #98 is fixed.
    withTempRedshiftTable("roundtrip_save_and_load") { tableName =>
      write(
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), TestUtils.expectedData)
    }
  }

  test("roundtrip save and load with uppercase column names") {
    testRoundtripSaveAndLoad(
      s"roundtrip_write_and_read_with_uppercase_column_names",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("A", IntegerType) :: Nil)),
      expectedSchemaAfterLoad = Some(StructType(StructField("a", IntegerType) :: Nil)))
  }

  test("save with column names that are reserved words") {
    testRoundtripSaveAndLoad(
      s"save_with_column_names_that_are_reserved_words",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("table", IntegerType) :: Nil)))
  }

  test("save with one empty partition (regression test for #96)") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 2),
      StructType(StructField("foo", IntegerType) :: Nil))
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array(Row(1))))
    testRoundtripSaveAndLoad(s"save_with_one_empty_partition", df)
  }

  test("save with all empty partitions (regression test for #96)") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq.empty[Row], 2),
      StructType(StructField("foo", IntegerType) :: Nil))
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array.empty[Row]))
    testRoundtripSaveAndLoad(s"save_with_all_empty_partitions", df)
    // Now try overwriting that table. Although the new table is empty, it should still overwrite
    // the existing table.
    val df2 = df.withColumnRenamed("foo", "bar")
    testRoundtripSaveAndLoad(
      s"save_with_all_empty_partitions", df2, saveMode = SaveMode.Overwrite)
  }

  test("informative error message when saving a table with string that is longer than max length") {
    withTempRedshiftTable("error_message_when_string_too_long") { tableName =>
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 512))),
        StructType(StructField("A", StringType) :: Nil))
      val e = intercept[SQLException] {
        write(df)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
      assert(e.getMessage.contains("while loading data into Redshift"))
    }
  }

  test("full timestamp precision is preserved in loads (regression test for #214)") {
    val timestamps = Seq(
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 1),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 10),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 100),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 1000))
    testRoundtripSaveAndLoad(
      s"full_timestamp_precision_is_preserved",
      sqlContext.createDataFrame(sc.parallelize(timestamps.map(Row(_))),
        StructType(StructField("ts", TimestampType) :: Nil))
    )
  }
}

@ExtendedRedshiftTest
class AvroRedshiftWriteSuite extends BaseRedshiftWriteSuite {
  override protected val tempformat: String = "AVRO"

  test("informative error message when saving with column names that contain spaces (#84)") {
    intercept[IllegalArgumentException] {
      testRoundtripSaveAndLoad(
        s"error_when_saving_column_name_with_spaces",
        sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
          StructType(StructField("column name with spaces", IntegerType) :: Nil)))
    }
  }
}

@ExtendedRedshiftTest
class CSVRedshiftWriteSuite extends BaseRedshiftWriteSuite {
  override protected val tempformat: String = "CSV"

  test("save with column names that contain spaces (#84)") {
    testRoundtripSaveAndLoad(
      s"save_with_column_names_that_contain_spaces",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("column name with spaces", IntegerType) :: Nil)))
  }
}

@ExtendedRedshiftTest
class CSVGZIPRedshiftWriteSuite extends IntegrationSuiteBase {
  // Note: we purposely don't inherit from BaseRedshiftWriteSuite because we're only interested in
  // testing basic functionality of the GZIP code; the rest of the write path should be unaffected
  // by compression here.

  override protected def write(df: DataFrame): DataFrameWriter[Row] =
    super.write(df).option("tempformat", "CSV GZIP")

  test("roundtrip save and load") {
    // This test can be simplified once #98 is fixed.
    withTempRedshiftTable("roundtrip_save_and_load") { tableName =>
      write(
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), TestUtils.expectedData)
    }
  }
}
