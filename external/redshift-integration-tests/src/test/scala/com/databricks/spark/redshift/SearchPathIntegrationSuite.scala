/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import java.sql.SQLException

import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}

class SearchPathIntegrationSuite extends IntegrationSuiteBase {
  private val testTable: String = s"search_path_test_table_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    conn.prepareStatement(s"drop table if exists $testTable").executeUpdate()
    conn.commit()
    createTestDataInRedshift(testTable)
  }

  override def afterAll(): Unit = {
    try {
      conn.prepareStatement(s"drop table if exists $testTable").executeUpdate()
      conn.commit()
    } finally {
      super.afterAll()
    }
  }

  protected override def read: DataFrameReader = {
    // leave out specifying "search_path" to the tests
    sqlContext.read
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("tempdir", tempDir)
      .option("forward_spark_s3_credentials", "true")
  }

  protected override def write(df: DataFrame): DataFrameWriter[Row] = {
    // leave out specifying "search_path" to the tests
    df.write
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("tempdir", tempDir)
      .option("forward_spark_s3_credentials", "true")
  }

  test("simple correct search_path") {
    assertResult(6) {
      read
        .option("search_path", s"$schemaName")
        .option("dbtable", testTable).load().count()
    }
  }

  test("complex correct search_path") {
    assertResult(6) {
      read
        .option("search_path", s"""Public, "$schemaName", '$$user'""")
        .option("dbtable", testTable).load().count()
    }
  }

  test("incorrect search_path") {
    try {
      read
        .option("search_path", "PUBLIC")
        .option("dbtable", testTable).load().count()
      fail("Should have failed with SQLException")
    } catch {
      case ex: SQLException =>
        assert(ex.toString.contains(
          s"""Invalid operation: relation "$testTable" does not exist"""))
      case NonFatal(ex) => fail("Expected SQLException, got: " + ex)
    }
  }

  test("non-existent schema") {
    try {
      read
        .option("search_path", "nonexistentfoo123")
        .option("dbtable", testTable).load().count()
      fail("Should have failed with SQLException")
    } catch {
      case ex: SQLException =>
        assert(ex.toString.contains(
          s"""Invalid operation: schema "nonexistentfoo123" does not exist"""))
      case NonFatal(ex) => fail("Expected SQLException, got: " + ex)
    }
  }

  test("garbage in search_path") {
    try {
      read
        .option("search_path", ",a;qe3212344")
        .option("dbtable", testTable).load().count()
      fail("Should have failed with IllegalArgumentException")
    } catch {
      case ex: IllegalArgumentException =>
        assert(ex.toString.contains(s"""Invalid syntax of search_path"""))
      case NonFatal(ex) => fail("Expected SQLException, got: " + ex)
    }
  }

  test("SQL injection in search_path") {
    try {
      read
        .option("search_path", s"$schemaName; drop table $testTable")
        .option("dbtable", testTable).load().count()
      fail("Should have failed with IllegalArgumentException")
    } catch {
      case ex: IllegalArgumentException =>
        assert(ex.toString.contains(s"""Invalid syntax of search_path"""))
      case NonFatal(ex) => fail("Expected IllegalArgumentException, got: " + ex)
    }
  }
}
