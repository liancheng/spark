/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import org.apache.spark.sql.{execution, Row}
import org.apache.spark.sql.types.LongType
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * End-to-end tests of functionality which only impacts the read path (e.g. filter pushdown).
 */
@ExtendedRedshiftTest
class RedshiftReadSuite extends IntegrationSuiteBase {

  private val test_table: String = s"read_suite_test_table_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
    conn.commit()
    createTestDataInRedshift(test_table)
  }

  override def afterAll(): Unit = {
    try {
      conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
      conn.commit()
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    read.option("dbtable", test_table).load().createOrReplaceTempView("test_table")
  }

  test("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    checkAnswer(
      sqlContext.sql("select * from test_table"),
      TestUtils.expectedData)
  }

  test("count() on DataFrame created from a Redshift table") {
    checkAnswer(
      sqlContext.sql("select count(*) from test_table"),
      Seq(Row(TestUtils.expectedData.length))
    )
  }

  test("count() on DataFrame created from a Redshift query") {
    val loadedDf =
    // scalastyle:off
      read.option("query", s"select * from $test_table where teststring = 'Unicode''s樂趣'").load()
    // scalastyle:on
    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("backslashes in queries/subqueries are escaped (regression test for #215)") {
    val loadedDf =
      read.option("query", s"select replace(teststring, '\\\\', '') as col from $test_table").load()
    checkAnswer(
      loadedDf.filter("col = 'asdf'"),
      Seq(Row("asdf"))
    )
  }

  test("Can load output when 'dbtable' is a subquery wrapped in parentheses") {
    // scalastyle:off
    val query =
      s"""
         |(select testbyte, testbool
         |from $test_table
         |where testbool = true
         | and teststring = 'Unicode''s樂趣'
         | and testdouble = 1234152.12312498
         | and testfloat = 1.0
         | and testint = 42)
      """.stripMargin
    // scalastyle:on
    checkAnswer(read.option("dbtable", query).load(), Seq(Row(1, true)))
  }

  test("Can load output when 'query' is specified instead of 'dbtable'") {
    // scalastyle:off
    val query =
      s"""
         |select testbyte, testbool
         |from $test_table
         |where testbool = true
         | and teststring = 'Unicode''s樂趣'
         | and testdouble = 1234152.12312498
         | and testfloat = 1.0
         | and testint = 42
      """.stripMargin
    // scalastyle:on
    checkAnswer(read.option("query", query).load(), Seq(Row(1, true)))
  }

  test("Can load output of Redshift aggregation queries") {
    checkAnswer(
      read.option("query", s"select testbool, count(*) from $test_table group by testbool").load(),
      Seq(Row(true, 1), Row(false, 2), Row(null, 3)))
  }

  test("multiple scans on same table") {
    // .rdd() forces the first query to be unloaded from Redshift
    val rdd1 = sqlContext.sql("select testint from test_table").rdd
    // Similarly, this also forces an unload:
    sqlContext.sql("select testdouble from test_table").rdd
    // If the unloads were performed into the same directory then this call would fail: the
    // second unload from rdd2 would have overwritten the integers with doubles, so we'd get
    // a NumberFormatException.
    rdd1.count()
  }

  test("DefaultSource supports simple column filtering") {
    checkAnswer(
      sqlContext.sql("select testbyte, testbool from test_table"),
      Seq(
        Row(null, null),
        Row(null, null),
        Row(0.toByte, null),
        Row(0.toByte, false),
        Row(1.toByte, false),
        Row(1.toByte, true)))
  }

  test("query with pruned and filtered scans") {
    // scalastyle:off
    checkAnswer(
      sqlContext.sql(
        """
          |select testbyte, testbool
          |from test_table
          |where testbool = true
          | and teststring = "Unicode's樂趣"
          | and testdouble = 1234152.12312498
          | and testfloat = 1.0
          | and testint = 42
        """.stripMargin),
      Seq(Row(1, true)))
    // scalastyle:on
  }

  test("RedshiftRelation implements Spark 1.6+'s unhandledFilters API") {
    assume(org.apache.spark.SPARK_VERSION.take(3) >= "1.6")
    val df = sqlContext.sql("select testbool from test_table where testbool = true")
    val physicalPlan = df.queryExecution.sparkPlan
    physicalPlan.collectFirst { case f: execution.FilterExec => f }.foreach { filter =>
      fail(s"Filter should have been eliminated:\n${df.queryExecution}")
    }
  }

  test("filtering based on date constants (regression test for #152)") {
    val date = TestUtils.toDate(year = 2015, zeroBasedMonth = 6, date = 3)
    val df = sqlContext.sql("select testdate from test_table")

    checkAnswer(df.filter(df("testdate") === date), Seq(Row(date)))
    // This query failed in Spark 1.6.0 but not in earlier versions. It looks like 1.6.0 performs
    // constant-folding, whereas earlier Spark versions would preserve the cast which prevented
    // filter pushdown.
    checkAnswer(df.filter("testdate = to_date('2015-07-03')"), Seq(Row(date)))
  }

  test("filtering based on timestamp constants (regression test for #152)") {
    val timestamp = TestUtils.toTimestamp(2015, zeroBasedMonth = 6, 1, 0, 0, 0, 1)
    val df = sqlContext.sql("select testtimestamp from test_table")

    checkAnswer(df.filter(df("testtimestamp") === timestamp), Seq(Row(timestamp)))
    // This query failed in Spark 1.6.0 but not in earlier versions. It looks like 1.6.0 performs
    // constant-folding, whereas earlier Spark versions would preserve the cast which prevented
    // filter pushdown.
    checkAnswer(df.filter("testtimestamp = '2015-07-01 00:00:00.001'"), Seq(Row(timestamp)))
  }

  test("read special float values (regression test for #261)") {
    val tableName = s"roundtrip_special_float_values_$randomSuffix"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE TABLE $tableName (x real)")
      conn.createStatement().executeUpdate(
        s"INSERT INTO $tableName VALUES ('NaN'), ('Infinity'), ('-Infinity')")
      conn.commit()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      // Due to #98, we use Double here instead of float:
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).map(x => Row.apply(x)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("read special double values (regression test for #261)") {
    val tableName = s"roundtrip_special_double_values_$randomSuffix"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE TABLE $tableName (x double precision)")
      conn.createStatement().executeUpdate(
        s"INSERT INTO $tableName VALUES ('NaN'), ('Infinity'), ('-Infinity')")
      conn.commit()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).map(x => Row.apply(x)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("read records containing escaped characters") {
    withTempRedshiftTable("records_with_escaped_characters") { tableName =>
      conn.createStatement().executeUpdate(
        s"CREATE TABLE $tableName (x text)")
      conn.createStatement().executeUpdate(
        s"""INSERT INTO $tableName VALUES ('a\\nb'), ('\\\\'), ('"')""")
      conn.commit()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq("a\nb", "\\", "\"").map(x => Row.apply(x)))
    }
  }

  test("read result of approximate count(distinct) query (#300)") {
    val df = read
      .option("query", s"select approximate count(distinct testbool) as c from $test_table")
      .load()
    assert(df.schema.fields(0).dataType === LongType)
  }

  test("properly escape literals in filter pushdown (SC-5504)") {
    checkAnswer(
      sqlContext.sql("select count(1) from test_table where testint = 4141214"),
      Seq(Row(1))
    )
    checkAnswer(
      sqlContext.sql("select count(1) from test_table where testint = 7"),
      Seq(Row(0))
    )
    checkAnswer(
      sqlContext.sql("select testint from test_table where testint = 42"),
      Seq(Row(42), Row(42))
    )

    checkAnswer(
      sqlContext.sql("select count(1) from test_table where teststring = 'asdf'"),
      Seq(Row(1))
    )
    checkAnswer(
      sqlContext.sql("select count(1) from test_table where teststring = 'alamakota'"),
      Seq(Row(0))
    )
    checkAnswer(
      sqlContext.sql("select teststring from test_table where teststring = 'asdf'"),
      Seq(Row("asdf"))
    )

    checkAnswer(
      sqlContext.sql("select count(1) from test_table where teststring = 'a\\'b'"),
      Seq(Row(0))
    )
    checkAnswer(
      sqlContext.sql("select teststring from test_table where teststring = 'a\\'b'"),
      Seq()
    )

    // scalastyle:off
    checkAnswer(
      sqlContext.sql("select count(1) from test_table where teststring = 'Unicode\\'s樂趣'"),
      Seq(Row(1))
    )
    checkAnswer(
      sqlContext.sql("select teststring from test_table where teststring = \"Unicode's樂趣\""),
      Seq(Row("Unicode's樂趣"))
    )
    // scalastyle:on

    checkAnswer(
      sqlContext.sql("select count(1) from test_table where teststring = 'a\\\\b'"),
      Seq(Row(0))
    )
    checkAnswer(
      sqlContext.sql("select teststring from test_table where teststring = 'a\\\\b'"),
      Seq()
    )

    checkAnswer(
      sqlContext.sql(
        "select count(1) from test_table where teststring = 'Ba\\\\ckslash\\\\'"),
      Seq(Row(1))
    )
    checkAnswer(
      sqlContext.sql(
        "select teststring from test_table where teststring = \"Ba\\\\ckslash\\\\\""),
      Seq(Row("Ba\\ckslash\\"))
    )
  }
}
