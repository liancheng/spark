/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import org.apache.spark.sql.Row

class DataTypesIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table")
    } finally {
      super.afterAll()
    }
  }

  def checkTestTable(expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = read
      .option("query", s"select * from $test_table order by i")
      .load()
    checkAnswer(loadedDf, expectedAnswer)
  }

  test("Test BOOLEAN") {
    jdbcUpdate(s"drop table if exists $test_table")
    jdbcUpdate(s"create table $test_table(i int, v boolean)")
    jdbcUpdate(s"insert into $test_table values(1, false),(2, true),(3, null)")
    checkTestTable(
      Seq(Row(1, false), Row(2, true), Row(3, null)))
  }

  test("Test TIMESTAMP") {
    jdbcUpdate(s"drop table if exists $test_table")
    jdbcUpdate(s"create table $test_table(i int, v timestamp)")
    jdbcUpdate(s"""insert into $test_table
                  |values (1, '2013-04-05 12:01:02 -02:00'),
                  |       (2, '2013-04-05 18:01:02.123 +02:00'),
                  |       (3, null)""".stripMargin)
    checkTestTable(
      Seq(Row(1, TestUtils.toTimestamp(2013, 3, 5, 12, 1, 2, 0)),
        Row(2, TestUtils.toTimestamp(2013, 3, 5, 18, 1, 2, 123)),
        Row(3, null)))
  }

  test("Test DATE") {
    jdbcUpdate(s"drop table if exists $test_table")
    jdbcUpdate(s"create table $test_table(i int, v date)")
    jdbcUpdate(s"insert into $test_table values(1, '1900-01-01'),(2, '2013-04-05'),(3, null)")
    checkTestTable(
      Seq(Row(1, TestUtils.toDate(1900, 0, 1)),
        Row(2, TestUtils.toDate(2013, 3, 5)),
        Row(3, null)))
  }
}
