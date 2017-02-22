/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift.pushdown

import com.databricks.spark.redshift.IntegrationSuiteBase

import org.apache.spark.sql.Row


/**
 * This suite is meant for testing the simple filter pushdown code that's based on the public API.
 */
class FilterPushdownIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"

  // Values used for comparison
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "Redshift")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"drop table if exists $test_table")
    jdbcUpdate(s"create table $test_table(i int, s varchar(256))")
    jdbcUpdate(s"""insert into $test_table
                  |values(null, 'Hello'), (2, 'Redshift'), (3, 'Spark'), (4, null)""".stripMargin)
  }

  test("Test Simple Comparisons") {
    testFilter("s = 'Hello'",
      s"""("s" IS NOT NULL) AND ("s" = 'Hello')""",
      Seq(row1))
    testFilter("i > 2",
      s"""("i" IS NOT NULL) AND ("i" > 2)""",
      Seq(row3, row4))
    testFilter("i < 3",
      s"""("i" IS NOT NULL) AND ("i" < 3)""",
      Seq(row2))
  }

  test("Test >= and <=") {
    testFilter("i >= 2",
      s"""("i" IS NOT NULL) AND ("i" >= 2)""",
      Seq(row2, row3, row4))
    testFilter("i <= 3",
      s"""("i" IS NOT NULL) AND ("i" <= 3)""",
      Seq(row2, row3))
  }

  test("Test logical operators") {
    testFilter("i >= 2 AND i <= 3",
      s"""("i" IS NOT NULL) AND ("i" >= 2) AND ("i" <= 3)""",
      Seq(row2, row3))
    testFilter("NOT i = 3",
      s"""("i" IS NOT NULL) AND (NOT ("i" = 3))""",
      Seq(row2, row4))
    testFilter("NOT i = 3 OR i IS NULL",
      s"""((NOT ("i" = 3)) OR ("i" IS NULL))""",
      Seq(row1, row2, row4))
    testFilter("i IS NULL OR i > 2 AND s IS NOT NULL",
      s"""(("i" IS NULL) OR (("i" > 2) AND ("s" IS NOT NULL)))""",
      Seq(row1, row3))
  }

  test("Test IN") {
    testFilter("i IN (2, 3)",
      s"""("i" IN (2, 3))""",
      Seq(row2, row3))
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table")
    } finally {
      super.afterAll()
    }
  }

  /**
   * Verify that the filter is pushed down by looking at the generated SQL,
   * and check the results are as expected
   */
  def testFilter(filter: String, expectedWhere: String, expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = read
      .option("dbtable", s"$test_table")
      .load().filter(filter).sort("i")
    val expectedQuery = s"""SELECT "i", "s" FROM "$test_table" WHERE $expectedWhere"""
    testPushdownDF(loadedDf, expectedAnswer, refBasicSQL = Some(expectedQuery), refFullSQL = None)
  }
}
