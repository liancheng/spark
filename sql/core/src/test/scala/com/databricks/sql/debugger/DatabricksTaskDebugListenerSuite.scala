/* Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.sql.debugger

import java.io.File

import scala.concurrent.duration._

import com.databricks.sql.DatabricksSQLConf
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class DatabricksTaskDebugListenerSuite
  extends QueryTest
  with SharedSQLContext
  with SQLTestUtils
  with Eventually {

  val CART_PROD_INPUT_SIZE = 100000L
  var prevKillerOutputRatioThreshold = 0L
  var prevKillerMinTime = 0L

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    prevKillerOutputRatioThreshold = spark.sessionState.conf.getConf(
        DatabricksSQLConf.TASK_KILLER_OUTPUT_RATIO_THRESHOLD)
    prevKillerMinTime = spark.sessionState.conf.getConf(DatabricksSQLConf.TASK_KILLER_MIN_TIME)

    spark.sessionState.conf.setConf(DatabricksSQLConf.TASK_KILLER_OUTPUT_RATIO_THRESHOLD, 100L)
    spark.sessionState.conf.setConf(DatabricksSQLConf.TASK_KILLER_MIN_TIME, 5L)
  }

  protected override def afterAll(): Unit = {
    spark.sessionState.conf.setConf(DatabricksSQLConf.TASK_KILLER_OUTPUT_RATIO_THRESHOLD,
        prevKillerOutputRatioThreshold)
    spark.sessionState.conf.setConf(DatabricksSQLConf.TASK_KILLER_MIN_TIME, prevKillerMinTime)
    super.afterAll()
  }

  def waitForTaskEnd(): Unit = {
    eventually(timeout(60.seconds)) {
      assert(sparkContext.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  def testTaskTermination(sparkOp: => Unit): Unit = {
    try {
      val ex = intercept[SparkException] { sparkOp }
      assert(ex.getMessage().contains(DatabricksSQLConf.TASK_KILLER_OUTPUT_RATIO_THRESHOLD.key))
    } finally {
      waitForTaskEnd()
    }
  }

  test("Slow query termination: simple CartProd on Parquet tables.") {
    withTempDir { root =>
      val dir = new File(root, "pqL").getCanonicalPath
      spark.range(CART_PROD_INPUT_SIZE).write.parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView("pqL")

      testTaskTermination {
        sql(s"select * from pqL cross join pqL").toDF("a", "b").agg(sum("a"), sum("b")).collect()
      }
    }
  }

  test("Slow query termination: multiple shuffles.") {
    withTempDir { root =>
      val dirL = new File(root, "pqL").getCanonicalPath
      spark.range(CART_PROD_INPUT_SIZE).write.parquet(dirL)
      spark.read.parquet(dirL).createOrReplaceTempView("pqL")

      val dirM = new File(root, "pqM").getCanonicalPath
      spark.range(CART_PROD_INPUT_SIZE).write.parquet(dirM)
      spark.read.parquet(dirM).createOrReplaceTempView("pqM")

      testTaskTermination {
        sql("select 1 as A from pqM").repartition(3)
          .crossJoin(sql("select 2 as B from pqM")).repartition(2)
          .crossJoin(sql("select 3 as C from pqL")).repartition(3)
          .filter("A = 1 AND B = 2 AND C = 3").collect()
      }
    }
  }

  test("Slow query termination: range without codegen.") {
    testTaskTermination {
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        spark.range(0, CART_PROD_INPUT_SIZE, 1).crossJoin(spark.range(0, CART_PROD_INPUT_SIZE, 1))
          .toDF("a", "b").agg(sum("a"), sum("b")).collect()
      }
    }
  }

  test("Slow query termination: range with codegen.") {
    testTaskTermination {
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        spark.range(0, CART_PROD_INPUT_SIZE, 1).crossJoin(spark.range(0, CART_PROD_INPUT_SIZE, 1))
          .toDF("a", "b").agg(sum("a"), sum("b")).collect()
      }
    }
  }
}
