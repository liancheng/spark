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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.test.TestHiveContext


/**
 * This suite checks the results, as well as Redshift SQL that is generated by the advanced pushdown
 * infrastructure. The tests use hardcoded data and the canonical results are hand-written.
 *
 * @note All tests are run twice, exercising both the simple and advanced pushdown code paths,
 *       and verify that the results are the same.
 */
class AdvancedPushdownIntegrationSuite extends IntegrationSuiteBase {

  override protected val createSqlContextBeforeEach = false

  private val test_table1 = s"test_table_simple1_$randomSuffix"
  private val test_table2 = s"test_table_simple2_$randomSuffix"

  // Values used for comparison
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "Redshift")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

  // Join Table
  private val row1b = Row(null, 1)
  private val row2b = Row(2, 2)
  private val row3b = Row(3, 2)
  private val row4b = Row(4, 3)

  override def beforeAll(): Unit = {
    super.beforeAll()

    /* Do it once in before all, override doing it in beforeEach */
    sqlContext = new TestHiveContext(sc, loadTestTables = false)
    sparkSession = sqlContext.sparkSession

    jdbcUpdate(s"drop table if exists $test_table1")
    jdbcUpdate(s"drop table if exists $test_table2")
    jdbcUpdate(s"create table $test_table1(i int, s varchar(256))")
    jdbcUpdate(s"create table $test_table2(o int, p int)")
    jdbcUpdate(
      s"""insert into $test_table1 values
         |(null, 'Hello'),
         |(2, 'Redshift'),
         |(3, 'Spark'),
         |(4, null)
       """.stripMargin)
    jdbcUpdate(
      s"""insert into $test_table2 values
         |(null, 1),
         |(2, 2 ),
         |(4, 3),
         |(4, 5),
         |(3, 2),
         |(null, 0),
         |(4, 3)
       """.stripMargin)
    conn.commit()

    val df1 = read
      .option("dbtable", test_table1)
      .load()

    val df2 = read
      .option("dbtable", test_table2)
      .load()

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
  }

  test("Basic join") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT first.s, second.p
           |FROM df1 first JOIN df2 second ON first.i = second.p
         """.stripMargin,
      refFullSQL = Some(
        s"""
            |SELECT ("subquery_1"."i") AS "subquery_1_col_0" FROM
            |       (SELECT * FROM
            |               (SELECT * FROM "test_table_simple1 "
            |       ) AS "subquery_0"
            |       WHERE ("subquery_0"."i" IS NOT NULL)
            |)""".stripMargin),
      refResult =
        Seq(Row("Redshift", 2), Row("Redshift", 2), Row("Spark", 3), Row("Spark", 3))
      )
  }

  test("Basic aggregation") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT p, count(distinct o) AS avg
           |FROM df2
           |GROUP BY p
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "o", "p" FROM "test_table_simple2 "
        """.stripMargin),
      refResult =
        Seq(Row(0, 0), Row(1, 0), Row(2, 2), Row(3, 1), Row(5, 1))
      )
  }

  test("Global count aggregation") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT count(*) FROM df2
           |WHERE o = 4
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT count(*) FROM
           |    (SELECT * FROM
           |     (SELECT * FROM "test_table_simple2 ") AS "subquery_0"
           |      WHERE (("subquery_0"."o" IS NOT NULL) AND ("subquery_0"."o" = 4)))
        """.stripMargin),
      refResult = Seq(Row(3))
    )
  }

  test("Basic filters") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT *
           |FROM df2
           |WHERE p > 1 AND p < 3
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "o", "p" FROM
           |       (SELECT * FROM
           |               (SELECT * FROM "test_table_simple2 "
           |       ) AS "subquery_0"
           |       WHERE ((("subquery_0"."p" IS NOT NULL) AND ("subquery_0"."p" > 1))
           |             AND ("subquery_0"."p" < 3)))
         """.stripMargin),
      refResult = Seq(Row(2, 2), Row(3, 2))
    )
  }

  test("Basic sort") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT * FROM df2
           |ORDER BY o ASC NULLS LAST, p DESC
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "o", "p" FROM
           |       (SELECT * FROM
           |               (SELECT * FROM "test_table_simple2 "
           |       ) AS "subquery_0"
           |       ORDER BY ("subquery_0"."o") ASC NULLS LAST,
           |                ("subquery_0"."p") DESC NULLS LAST)
         """.stripMargin),
      refResult =
        Seq(Row(2, 2), Row(3, 2), Row(4, 5), Row(4, 3), Row(4, 3), Row(null, 1), Row(null, 0)))
  }

  test("Subquery with limit") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT count(*)
           |FROM (SELECT p FROM (select * FROM df2 LIMIT 1))
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "o", "p" FROM
           |       (SELECT * FROM
           |               (SELECT * FROM "test_table_simple2 "
           |       ) AS "subquery_0"
           |       LIMIT 1)
         """.stripMargin),
      refResult =
        Seq(Row(1))
    )
  }

  test("Filter, Limit and Sort") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT *
           |FROM df2
           |WHERE p > 1 AND p < 3
           |ORDER BY p DESC, o ASC LIMIT 1
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "o", "p" FROM
           |       (SELECT * FROM
           |               (SELECT * FROM
           |                       (SELECT * FROM
           |                               (SELECT * FROM "test_table_simple2 "
           |                       ) AS "subquery_0"
           |                WHERE ((("subquery_0"."p" IS NOT NULL) AND ("subquery_0"."p" > 1))
           |                      AND ("subquery_0"."p" < 3))
           |               ) AS "subquery_1"
           |        ORDER BY ("subquery_1"."p") DESC NULLS LAST, ("subquery_1"."o") ASC NULLS FIRST
           |       ) AS "subquery_2"
           |       LIMIT 1)
         """.stripMargin),
      refResult =
        Seq(Row(2, 2))
    )
  }

  test("Nested query with column alias") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT f, o
           |FROM (SELECT o, p AS f FROM df2 WHERE p > 1 AND p < 3) AS foo
           |ORDER BY f, o DESC
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "subquery_2_col_0", "subquery_2_col_1" FROM
           |       (SELECT * FROM
           |               (SELECT ("subquery_1"."p") AS "subquery_2_col_0",
           |                       ("subquery_1"."o") AS "subquery_2_col_1" FROM
           |                       (SELECT * FROM
           |                               (SELECT * FROM "test_table_simple2 "
           |                       ) AS "subquery_0"
           |                WHERE ((("subquery_0"."p" IS NOT NULL) AND ("subquery_0"."p" > 1))
           |                      AND ("subquery_0"."p" < 3))
           |               ) AS "subquery_1"
           |       ) AS "subquery_2"
           |       ORDER BY ("subquery_2"."subquery_2_col_0") ASC NULLS FIRST,
           |                ("subquery_2"."subquery_2_col_1") DESC NULLS LAST)
         """.stripMargin),
      refResult =
        Seq(Row(2, 3), Row(2, 2))
    )
  }

  test("Sum and RPAD") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT sum(i) AS hi, rpad(s, 10, "*") AS ho
           |FROM df1
           |GROUP BY s
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "i", "s" FROM "test_table_simple1 "
        """.stripMargin),
      refResult =
        Seq(Row(null, "Hello*****"), Row(2, "Redshift**"), Row(3, "Spark*****"), Row(4, null))
    )
  }

  test("NOT IN") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT DISTINCT o, p
           |FROM df2
           |WHERE o IS NULL OR o NOT IN (1, 2, 3)
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "o", "p" FROM
           |       (SELECT * FROM
           |               (SELECT * FROM "test_table_simple2 "
           |       ) AS "subquery_0"
           |       WHERE (("subquery_0"."o" IS NULL) OR NOT(("subquery_0"."o" IN (1, 2, 3)))))
        """.stripMargin),
      refResult =
        Seq(Row(null, 0), Row(null, 1), Row(4, 3), Row(4, 5))
    )
  }

  test("String expressions") {
    testPushdownDF(
      sparkSession.sql(
        s"""
           |SELECT i, s,
           |  Like(s, 'R%') AS tarts_with_R,
           |  Like(s, '%edshif%') AS contains_edshif,
           |  Like(s, '%t') AS ends_with_t
           |FROM df1
         """.stripMargin).
        where(col("s").startsWith("R") and col("s").contains("edshif") and col("s").endsWith("t")),
      refFullSQL = Some(
        s"""
           |SELECT "subquery_2_col_0", "subquery_2_col_1", "subquery_2_col_2",
           |       "subquery_2_col_3", "subquery_2_col_4" FROM
           |       (SELECT ("subquery_1"."i") AS "subquery_2_col_0",
           |               ("subquery_1"."s") AS "subquery_2_col_1",
           |               (("subquery_1"."s" LIKE 'R%')) AS "subquery_2_col_2",
           |               (("subquery_1"."s" LIKE '%edshif%')) AS "subquery_2_col_3",
           |               (("subquery_1"."s" LIKE '%t')) AS "subquery_2_col_4" FROM
           |               (SELECT * FROM
           |                       (SELECT * FROM "test_table_simple1 "
           |               ) AS "subquery_0"
           |        WHERE ((("subquery_0"."s" LIKE 'R%') AND ("subquery_0"."s" LIKE '%edshif%'))
           |              AND ("subquery_0"."s" LIKE '%t'))
           |       ) AS "subquery_1")
        """.stripMargin),
      refResult =
        Seq(Row(2, "Redshift", true, true, true))
    )
  }

  test("Unsupported expression for advanced pushdown") {
    testPushdownSQL(
      sql =
        s"""
           |SELECT *
           |FROM df1
           |WHERE cast(i as timestamp) > current_timestamp
         """.stripMargin,
      refFullSQL = Some(
        s"""
           |SELECT "i", "s" FROM "test_table_simple1 " WHERE ("i" IS NOT NULL)
        """.stripMargin),
      refResult =
        Seq.empty
    )
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table1")
      jdbcUpdate(s"drop table if exists $test_table2")
    } finally {
      super.afterAll()
    }
  }
}
