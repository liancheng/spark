/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift.pushdown

import scala.util.Random

import com.databricks.spark.redshift.IntegrationSuiteBase

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.functions.{asc, col, desc, lit}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.types._


/**
 * This suite is generally similar to `AdvancedPushdownIntegrationSuite`, but differs in that
 * it runs queries against tables consisting of randomly generated data. To check for correctness,
 * we run each query a third time, against Spark (so no Redshift involved), and compare the results.
 */
class RandomizedPushdownIntegrationSuite extends IntegrationSuiteBase {

  override protected val createSqlContextBeforeEach = false

  private val test_table1: String = s"test_table_1_$randomSuffix"
  private val test_table2: String = s"test_table_2_$randomSuffix"
  private val table_placeholder = "table_placeholder"

  private val numRows1 = 50
  private val numRows2 = 50

  override def beforeAll(): Unit = {
    super.beforeAll()

    /* Do it once in before all, override doing it in beforeEach */
    sqlContext = new TestHiveContext(sc, loadTestTables = false)
    sparkSession = sqlContext.sparkSession

    val st1 = new StructType(
      Array(StructField("id", IntegerType, nullable = true),
        StructField("randInt", IntegerType, nullable = true),
        StructField("randStr", StringType, nullable = true),
        StructField("randBool", BooleanType, nullable = true),
        StructField("randLong", LongType, nullable = true)))

    val st2 = new StructType(
      Array(StructField("id", IntegerType, nullable = true),
        StructField("randStr2", StringType, nullable = true),
        StructField("randStr3", StringType, nullable = true),
        StructField("randInt2", IntegerType, nullable = true)))

    val df1_spark = sqlContext.createDataFrame(
      sc.parallelize(1 to numRows1).map[Row](value => {
          val rand = new Random(System.nanoTime())
          Row(value,
            rand.nextInt(),
            rand.nextString(10),
            rand.nextBoolean(),
            rand.nextLong())
      }), st1)
    .cache()

    // Contains some nulls
    val df2_spark = sqlContext.createDataFrame(
      sc.parallelize(1 to numRows2).map[Row](value => {
          val rand = new Random(System.nanoTime())
          Row(value,
            rand.nextString(10),
            rand.nextString(5),
            {
              val r = rand.nextInt()
              if (r % 5 == 2) null else r
            })
        }), st2)
      .cache()

    try {
      write(df1_spark)
        .option("dbtable", test_table1)
        .mode(SaveMode.Overwrite)
        .save()

      write(df2_spark)
        .option("dbtable", test_table2)
        .mode(SaveMode.Overwrite)
        .save()

      val df1_redshift = read
        .option("dbtable", test_table1)
        .load()

      val df2_redshift = read
        .option("dbtable", test_table2)
        .load()

      df1_spark.createOrReplaceTempView("df_spark1")
      df2_spark.createOrReplaceTempView("df_spark2")
      df1_redshift.createOrReplaceTempView("df_redshift1")
      df2_redshift.createOrReplaceTempView("df_redshift2")
    } catch {
      case e: Exception =>
        jdbcUpdate(s"drop table if exists $test_table1")
        jdbcUpdate(s"drop table if exists $test_table2")
        throw (e)
    }
  }

  def table1DF_spark: DataFrame = sparkSession.sql("select * from df_spark1")
  def table2DF_spark: DataFrame = sparkSession.sql("select * from df_spark2")

  def table1DF_redshift: DataFrame = read.option("dbtable", test_table1).load()
  def table2DF_redshift: DataFrame = read.option("dbtable", test_table2).load()

  def testDF(df: DataFrame, refResults: Seq[Row], ref: Option[String]): Unit = {
    testPushdownDF(df, refResults, refBasicSQL = None, refFullSQL = ref)
  }

  def testSQL(sql: String, ref: Option[String]): Unit = {
    val sparkDF = sparkSession.sql(sql.replaceAll(s"""$table_placeholder""", "df_spark"))
    val redshiftDF = sparkSession.sql(sql.replaceAll(s"""$table_placeholder""", "df_redshift"))
    testDF(redshiftDF, sparkDF.collect(), ref)
  }

  test("Select all columns.") {
    testSQL(
      sql =
        s"""
           |SELECT * FROM ${table_placeholder}1
         """.stripMargin,
      ref = Some(
        s"""
           |SELECT "id", "randint", "randstr", "randbool", "randlong" FROM "test_table_1 "
         """.stripMargin)
    )
  }

  test("Join") {
    testSQL(
      sql =
        s"""
           |SELECT b.id, a.randInt
           |FROM ${table_placeholder}1 AS a INNER JOIN ${table_placeholder}2 AS b
           |      ON a.randBool = ISNULL(b.randInt2)
         """.stripMargin,
      ref = Some(
        s"""
           |SELECT "subquery_2_col_0", "subquery_2_col_1" FROM
           |       (SELECT ("subquery_1"."randint") AS "subquery_2_col_0",
           |               ("subquery_1"."randbool") AS "subquery_2_col_1" FROM
           |               (SELECT * FROM
           |                       (SELECT * FROM "test_table_1 "
           |               ) AS "subquery_0"
           |        WHERE ("subquery_0"."randbool" IS NOT NULL)
           |       ) AS "subquery_1")
         """.stripMargin)
    )
  }

  test("Concatenation and LPAD") {
    testSQL(
      sql =
        s"""
           |SELECT concat(randStr2, randStr3) AS c, lpad(randStr2, 5, '%') AS l
           |FROM ${table_placeholder}2
         """.stripMargin,
      ref = Some(
        s"""
           |SELECT "subquery_1_col_0", "subquery_1_col_1" FROM
           |       (SELECT
           |        (CONCAT("subquery_0"."randstr2", "subquery_0"."randstr3"))
           |          AS "subquery_1_col_0",
           |        (LPAD("subquery_0"."randstr2", 5, '%')) AS "subquery_1_col_1" FROM
           |               (SELECT * FROM "test_table_2 "
           |       ) AS "subquery_0")
        """.stripMargin)
    )
  }

  test("Translate") {
    testSQL(
      sql =
        s"""
           |SELECT translate(randStr2, 'sd', 'po') AS l
           |FROM ${table_placeholder}2
         """.stripMargin,
      ref = Some(
        s"""
           |SELECT "subquery_1_col_0" FROM
           |       (SELECT (TRANSLATE("subquery_0"."randstr2", 'sd', 'po'))
           |         AS "subquery_1_col_0" FROM
           |               (SELECT * FROM "test_table_2 "
           |       ) AS "subquery_0")
        """.stripMargin)
    )
  }

  test("Join and Max Aggregation") {
    testSQL(
      sql =
        s"""
           |SELECT a.id, max(b.randInt2)
           |FROM ${table_placeholder}1 AS a INNER JOIN ${table_placeholder}2 AS b
           |      ON cast(a.randInt/5 AS integer) = cast(b.randInt2/5 AS integer)
           |GROUP BY a.id
         """.stripMargin,
      ref = Some (
        s"""
           |SELECT "subquery_2_col_0", "subquery_2_col_1" FROM
           |       (SELECT ("subquery_1"."id") AS "subquery_2_col_0",
           |               ("subquery_1"."randint") AS "subquery_2_col_1" FROM
           |               (SELECT * FROM
           |                       (SELECT * FROM "test_table_1 "
           |               ) AS "subquery_0"
           |        WHERE ("subquery_0"."randint" IS NOT NULL)
           |       ) AS "subquery_1")
        """.stripMargin)
    )
  }

  test("Join on multiple conditions") {
    testSQL(
      sql =
        s"""
           |SELECT b.randStr2
           |FROM ${table_placeholder}2 AS b
           |INNER JOIN ${table_placeholder}1 AS a on ISNULL(b.randInt2) = a.randBool
           |        AND a.randStr=b.randStr2
         """.stripMargin,
      ref = Some (
        s"""
           |SELECT "subquery_2_col_0", "subquery_2_col_1" FROM
           |        (SELECT ("subquery_1"."randstr2") AS "subquery_2_col_0",
           |                ("subquery_1"."randint2") AS "subquery_2_col_1" FROM
           |                (SELECT * FROM
           |                        (SELECT * FROM "test_table_2 "
           |                ) AS "subquery_0"
           |         WHERE ("subquery_0"."randstr2" IS NOT NULL)
           |        ) AS "subquery_1")
        """.stripMargin)
      // Note that we've flipped here the table order on purpose, because we're currently only
      // checking the SQL corresponding to the first encountered RedshiftRelation in the plan.
      // If that was table1 instead, then the generated WHERE clause would be:
      // isnotnull(randBool#8) && isnotnull(randStr#7)
      // (introduced by InferFiltersFromConstraints in Optimizer.scala)
      // but the order of those two expressions would be non-deterministic across, causing this
      // test to be flaky. See SC-5957
    )
  }

  test("Aggregate by multiple columns") {
    testSQL(
      sql =
        s"""
           |SELECT max(randLong) AS m
           |FROM ${table_placeholder}1
           |GROUP BY randInt,randBool
         """.stripMargin,
      ref = Some(
        s"""
           |SELECT "subquery_1_col_0", "subquery_1_col_1", "subquery_1_col_2" FROM
           |       (SELECT ("subquery_0"."randint") AS "subquery_1_col_0",
           |               ("subquery_0"."randbool") AS "subquery_1_col_1",
           |               ("subquery_0"."randlong") AS "subquery_1_col_2" FROM
           |               (SELECT * FROM "test_table_1 "
           |       ) AS "subquery_0")
        """.stripMargin))
  }

  test("Scalar subqueries") {
    testSQL(
      sql =
        s"""
           |SELECT (SELECT count(*) FROM (SELECT * FROM ${table_placeholder}1 LIMIT 1)) AS one,
           |       (SELECT count(*) FROM ${table_placeholder}1 where id < 2) AS two
           |FROM ${table_placeholder}1 LIMIT 1""".stripMargin,
      ref = None
    )
  }

  test("DataFrame simple operations") {
    testDF(
      df =
        table1DF_redshift.
          orderBy("randstr").withColumn("zero", col("randint") * lit(0)).where("zero != 0"),
      refResults =
        Seq.empty[Row],
      ref = Some(
        s"""
           |SELECT "subquery_3_col_0", "subquery_3_col_1", "subquery_3_col_2", "subquery_3_col_3",
           |       "subquery_3_col_4", "subquery_3_col_5" FROM
           |       (SELECT ("subquery_2"."id") AS "subquery_3_col_0",
           |               ("subquery_2"."randint") AS "subquery_3_col_1",
           |               ("subquery_2"."randstr") AS "subquery_3_col_2",
           |               ("subquery_2"."randbool") AS "subquery_3_col_3",
           |               ("subquery_2"."randlong") AS "subquery_3_col_4",
           |               (("subquery_2"."randint" * 0)) AS "subquery_3_col_5" FROM
           |               (SELECT * FROM
           |                       (SELECT * FROM
           |                               (SELECT * FROM "test_table_1 "
           |                       ) AS "subquery_0"
           |                WHERE (("subquery_0"."randint" IS NOT NULL)
           |                      AND NOT((("subquery_0"."randint" * 0) = 0)))
           |               ) AS "subquery_1"
           |        ORDER BY ("subquery_1"."randstr") ASC NULLS FIRST
           |       ) AS "subquery_2")
         """.stripMargin)
    )
  }


  test("DataFrame more operations") {
    testDF(
      df =
        table1DF_redshift.
          orderBy(asc("randstr"), desc("randlong")).where("randbool = true").limit(3).
          withColumnRenamed("randbool", "flag").drop("randint"),
      refResults =
        table1DF_spark.
          orderBy(asc("randstr"), desc("randlong")).where("randbool = true").limit(3).
          withColumnRenamed("randbool", "flag").drop("randint").
          collect(),
      ref = Some(
        s"""
           |SELECT "subquery_5_col_0", "subquery_5_col_1", "subquery_5_col_2", "subquery_5_col_3"
           |        FROM (SELECT ("subquery_4"."subquery_2_col_0") AS "subquery_5_col_0",
           |               ("subquery_4"."subquery_2_col_1") AS "subquery_5_col_1",
           |               ("subquery_4"."subquery_2_col_2") AS "subquery_5_col_2",
           |               ("subquery_4"."subquery_2_col_3") AS "subquery_5_col_3" FROM
           |               (SELECT * FROM
           |                       (SELECT * FROM
           |                               (SELECT ("subquery_1"."id") AS "subquery_2_col_0",
           |                                       ("subquery_1"."randstr") AS "subquery_2_col_1",
           |                                       ("subquery_1"."randbool") AS "subquery_2_col_2",
           |                                       ("subquery_1"."randlong") AS "subquery_2_col_3"
           |                                       FROM (SELECT * FROM
           |                                               (SELECT * FROM "test_table_1 "
           |                                       ) AS "subquery_0"
           |                                WHERE (("subquery_0"."randbool" IS NOT NULL)
           |                                      AND ("subquery_0"."randbool" = true))
           |                               ) AS "subquery_1"
           |                       ) AS "subquery_2"
           |                ORDER BY ("subquery_2"."subquery_2_col_1") ASC NULLS FIRST,
           |                         ("subquery_2"."subquery_2_col_3") DESC NULLS LAST
           |               ) AS "subquery_3"
           |        LIMIT 3
           |       ) AS "subquery_4")
         """.stripMargin)
    )
  }

  test("DataFrame complex filter expressions") {
    testDF(
      df =
        table1DF_redshift.
          withColumnRenamed("randbool", "to_be").
          where("to_be or not to_be").
          drop("randstr", "randint", "randlong", "to_be").
          distinct().
          where((col("id") * lit(2)) < (col("id") + lit(5)) or col("id") === lit(10)),
      refResults =
        Seq(1, 2, 3, 4, 10).map(Row(_)),
      ref = Some(
        s"""
           |SELECT "subquery_2_col_0" FROM
           |       (SELECT ("subquery_1"."id") AS "subquery_2_col_0" FROM
           |               (SELECT * FROM
           |                       (SELECT * FROM "test_table_1 "
           |               ) AS "subquery_0"
           |        WHERE (("subquery_0"."randbool" OR NOT("subquery_0"."randbool"))
           |              AND ((("subquery_0"."id" * 2) < ("subquery_0"."id" + 5))
           |              OR ("subquery_0"."id" = 10)))
           |       ) AS "subquery_1")
         """.stripMargin)
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
