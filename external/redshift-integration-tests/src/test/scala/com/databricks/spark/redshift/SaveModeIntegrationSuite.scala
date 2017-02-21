/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * End-to-end tests of [[SaveMode]] behavior.
 */
@ExtendedRedshiftTest
class SaveModeIntegrationSuite extends IntegrationSuiteBase {
  test("SaveMode.Overwrite with schema-qualified table name (#97)") {
    withTempRedshiftTable("overwrite_schema_qualified_table_name") { tableName =>
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil))
      // Ensure that the table exists:
      write(df)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, s"$schemaName.$tableName"))
      // Try overwriting that table while using the schema-qualified table name:
      write(df)
        .option("dbtable", s"$schemaName.$tableName")
        .mode(SaveMode.Overwrite)
        .save()
    }
  }

  test("SaveMode.Overwrite with non-existent table") {
    testRoundtripSaveAndLoad(
      s"overwrite_non_existent_table$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil)),
      saveMode = SaveMode.Overwrite)
  }

  test("SaveMode.Overwrite with existing table") {
    withTempRedshiftTable("overwrite_existing_table") { tableName =>
      // Create a table to overwrite
      write(sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil)))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))

      val overwritingDf =
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
      write(overwritingDf)
        .option("dbtable", tableName)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), TestUtils.expectedData)
    }
  }

  // TODO:test overwrite that fails.

  test("Append SaveMode doesn't destroy existing data") {
    withTempRedshiftTable("append_doesnt_destroy_existing_data") { tableName =>
      createTestDataInRedshift(tableName)
      val extraData = Seq(
        Row(2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L,
          24.toShort, "___|_123", null))

      write(sqlContext.createDataFrame(sc.parallelize(extraData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .mode(SaveMode.Append)
        .saveAsTable(tableName)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        TestUtils.expectedData ++ extraData)
    }
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    withTempRedshiftTable("respect_savemode_error_if_exists") { tableName =>
      val rdd = sc.parallelize(TestUtils.expectedData)
      val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
      createTestDataInRedshift(tableName) // to ensure that the table already exists

      // Check that SaveMode.ErrorIfExists throws an exception
      val e = intercept[Exception] {
        write(df)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .saveAsTable(tableName)
      }
      assert(e.getMessage.contains("exists"))
    }
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    withTempRedshiftTable("do_nothing_when_savemode_ignore") { tableName =>
      val rdd = sc.parallelize(TestUtils.expectedData.drop(1))
      val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
      createTestDataInRedshift(tableName) // to ensure that the table already exists
      write(df)
        .option("dbtable", tableName)
        .mode(SaveMode.Ignore)
        .saveAsTable(tableName)

      // Check that SaveMode.Ignore does nothing
      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        TestUtils.expectedData)
    }
  }
}
