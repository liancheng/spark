/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql

import org.apache.spark.SparkException
import org.apache.spark.api.java.function.{FilterFunction, MapFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.DatasetClosureTranslationSuite.{A, B, Inner, JavaFilter, JavaMap, NonCaseClass}
import org.apache.spark.sql.execution.MapElementsExec
import org.apache.spark.sql.execution.closure.TranslateClosureOptimizerRule
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Closure translation tries to play conservatively. It fallbacks to implementation without closure
 * translation if anything wrong happens.
 *
 * This test covers:
 *  1. Test whether closure translation fallback is triggered when:
 *     a. Config flag disabled, or
 *     b. making closure translation introduces extra performance penalty, or
 *     c. closure contains unsupported byte code, or
 *     d. the closure input argument's schema no longer matches the logical plan's schema, or
 *     e. closure contains unknown function calls, or
 *     f. closure may contains a while loop that cannot be translated to an expression, or
 *     g. other unexpected failure during closure translation..
 *  2. Test whether closure translation works in typical Dataset typed map or typed filter flow.
 */
class DatasetClosureTranslationSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val rand = new java.util.Random()

  // Creates a random set ranging from (-100, 100)
  private lazy val rdd: RDD[Int] =
    sparkContext.makeRDD((0 to 10000).toSeq.map(_ => rand.nextInt(200) - 200))

  test("closure translation is disable") {
    withSQLConf(TranslateClosureOptimizerRule.CONFIG_KEY -> "false") {
      val closure = (v: Int) => v * 2
      val query = Seq(1, 2).toDS().map(closure).queryExecution.sparkPlan
      val typedMap = query.find {
        case m: MapElementsExec => true
        case _ => false
      }
      assert(typedMap.isDefined)
    }
  }

  private def assertFallback[T: Encoder](query: Dataset[T]): Unit = {
    var expectedPlan: LogicalPlan = null
    withSQLConf(TranslateClosureOptimizerRule.CONFIG_KEY -> "false") {
      expectedPlan = copy(query).queryExecution.optimizedPlan
    }

    withSQLConf(TranslateClosureOptimizerRule.CONFIG_KEY -> "true") {
      comparePlans(expectedPlan, copy(query).as[T].queryExecution.optimizedPlan)
    }
  }

  // Make a copy of the Dataset to clear the cached query plans (which is lazy val).
  private def copy[T: Encoder](query: Dataset[T]): Dataset[T] = {
    query.as[T]
  }

  test("fallback if the logical plan's schema mismatch with argument type T's schema") {
    // "abc" doesn't match default name "value"
    assertFallback(rdd.toDF("abc").as[Int].map(_ * 2))
  }

  test("fallback if there is ambiguous column with same name") {
    assertFallback(rdd.toDF().join(rdd.toDF(), Seq("value", "value")).as[A].map(_.value * 2))
  }

  test("fallback if the closure contains unsupported byte code") {
    val closure = (v: Int) => {
      // Try catch is not supported by closure translation
      try {
        v / 0
      } catch {
        case ex: Throwable => 0
      }
    }
    assertFallback(rdd.toDS().map(closure))
  }

  test("fallback if the closure contains unsupported function call") {
    // Math.cos is not supported.
    assertFallback(rdd.toDS().map(Math.cos(_)))
  }

  test("fallback if the closure contains loop") {
    val closure = (v: Int) => {
      var index = 0
      while (index < v) {
        index += 1
      }
      index
    }
    assertFallback(rdd.toDS().map(closure))
  }

  private var unknownField = "3q424"
  test("fallback if the closure reference unrecognized static fields") {
    val closure = (v: Int) => { unknownField }
    assertFallback(rdd.toDS().map(closure))
  }

  test("fallback if the input argument type T is not a case class nor a primitive type") {
    assertFallback(
      Seq(new Array[Byte](10)).toDS().as[NonCaseClass](Encoders.kryo[NonCaseClass]).map(_.v))
  }

  test("fallback if closure references a method of input argument that is not a getter") {
    assertFallback(rdd.toDS().as[A].map(_.publicMethod))
  }

  test("fallback if closure translation may introduce extra serialization") {
    // Math.cos is not supported
    val unsupportedClosure = (v: Int) => Math.cos(v)
    assertFallback(rdd.toDS().map(unsupportedClosure).map(_.toInt * 2).map(unsupportedClosure))
  }

  private def checkTranslation[T: Encoder](query: Dataset[T]): Unit = {
    var datasetBeforeTranslation: Dataset[T] = null
    var planBeforeTranslation: LogicalPlan = null
    var resultBeforeTranslation: Seq[Row] = null
    withSQLConf(TranslateClosureOptimizerRule.CONFIG_KEY -> "false") {
      datasetBeforeTranslation = copy(query)
      planBeforeTranslation = datasetBeforeTranslation.queryExecution.optimizedPlan
      resultBeforeTranslation = datasetBeforeTranslation.toDF.collect().toSeq
    }

    withSQLConf(TranslateClosureOptimizerRule.CONFIG_KEY -> "true") {
      val datasetAfterTranslation = copy(query)
      val planAfterTranslation = datasetAfterTranslation.queryExecution.optimizedPlan
      // Makes sure the closure translations happens
      assert(normalizeExprIds(planBeforeTranslation) != normalizeExprIds(planAfterTranslation))
      checkAnswer(datasetAfterTranslation.toDF, resultBeforeTranslation)
    }
  }

  test("translates Scala typed map to untyped map") {
    val data = (0 to 1000).map { _ =>
      B(Inner(rand.nextInt(), rand.nextInt()), Inner(rand.nextInt(), rand.nextInt()),
        rand.nextLong(), rand.nextDouble())
    }
    val ds = sparkContext.makeRDD(data).toDS()
    checkTranslation(ds.map { b =>
      // This closure contains AssertNotNull, NPEOnNull, if-else, type cast and etc..
      if ((b.longValue + b.inner1.intValue1 - b.doubleValue) % 10 > b.inner2.intValue1 % 10 &&
        b.inner1.intValue2 <= b.inner2.intValue2) {
        b.inner1
      } else {
        b.inner2
      }
    })
  }

  test("divide by zero") {
    withSQLConf(TranslateClosureOptimizerRule.CONFIG_KEY -> "true") {
      intercept[SparkException] {
        rdd.toDS().map(_ / 0).show()
      }
    }
  }

  test("translates Java typed map to untyped map") {
    checkTranslation(rdd.toDS().map(new JavaMap(), implicitly[Encoder[Int]]))
  }

  test("translates Scala typed filter to untyped filter") {
    checkTranslation(rdd.toDS().filter(_ > 0))
  }

  test("translates Java typed filter to untyped filter") {
    checkTranslation(rdd.toDS().filter(new JavaFilter()))
  }

  test("translates a chain of typed operators") {
    val ds = rdd.toDS().map(_ * 2).filter(_ > 0).map(_ * 2)
    checkTranslation(ds)
  }
}

object DatasetClosureTranslationSuite {
  case class A(value: Int) {
    def publicMethod: Int = value
  }

  case class Inner(intValue1: Int, intValue2: Int)
  case class B(inner1: Inner, inner2: Inner, longValue: Long, doubleValue: Double)

  class NonCaseClass(val v: Int)

  class JavaMap extends MapFunction[Int, Int] {
    override def call(value: Int): Int = value * 2
  }

  class JavaFilter extends FilterFunction[Int] {
    override def call(value: Int): Boolean = value > 0
  }
}
