/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.catalyst.expressions.closure

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkFunSuite
import org.apache.spark.api.java.function.{FilterFunction, MapFunction}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, GetStructField, GreaterThan, If => IfExpression, Literal}
import org.apache.spark.sql.catalyst.expressions.closure.ClosureTranslationSuite.{A, B}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.types.{DoubleType, IntegerType, ObjectType, StructField, StructType}

class ClosureTranslationSuite extends SparkFunSuite {

  test("translates scala map function") {
    val f1 = (v: Int) => (v + 1) * 2 / 3
    val schema = StructType(Seq(StructField("a", IntegerType)))
    val expressions = ClosureTranslation.translateMap(f1, classOf[Int], schema)
    assert(expressions.length == 1)

    val expected = DivideLikeJVM((("a".attr + 1) * 2), Literal(3))
    assert(expressions(0) == expected)
  }

  test("translates java map function") {
    val f1 = new MapFunction[java.lang.Integer, java.lang.Integer] {
      override def call(value: Integer): Integer = {
        (value + 1) * 2 / 3
      }
    }
    val schema = StructType(Seq(StructField("a", IntegerType)))
    val expressions = ClosureTranslation.translateMap(f1, classOf[java.lang.Integer], schema)
    assert(expressions.length == 1)

    val expected =
      DivideLikeJVM(((NPEOnNull("a".attr, "a".attr) + 1) * 2), Literal(3))
    assert(expressions(0) == expected)
  }

  test("translates scala filter function") {
    val f = (v: A) => v.a1 > v.a2
    val expression = ClosureTranslation.translateFilter(f, classOf[A], schemaFor[A])
    assertEqual(
      expression,
      Some(GreaterThan(UnresolvedAttribute("a1"), UnresolvedAttribute("a2"))))
  }

  test("translates java filter function") {
    val f = new FilterFunction[A] {
      override def call(value: A): Boolean = value.a1 > value.a2
    }
    val expression = ClosureTranslation.translateFilter(f, classOf[A], schemaFor[A])
    assertEqual(
      expression,
      Some(GreaterThan(UnresolvedAttribute("a1"), UnresolvedAttribute("a2"))))
  }

  test("translates scala map function, flatten top level object") {
    val f = (v: A) => v
    val expressions = ClosureTranslation.translateMap(f, classOf[A], schemaFor[A])
    assert(expressions.length == 2)
    assertEqual(
      expressions.toList,
      UnresolvedAttribute("a1") :: UnresolvedAttribute("a2") :: Nil
    )
  }

  test("translates scala map function, flatten nested inner fields") {
    val f = (v: B) => v.b1
    val expressions = ClosureTranslation.translateMap(f, classOf[B], schemaFor[B])
    assert(expressions.length == 2)
    expressions.toList match {
      case GetStructField(AssertNotNull(UnresolvedAttribute("b1" :: Nil), _), _, Some("a1")) ::
        GetStructField(AssertNotNull(UnresolvedAttribute("b1" :: Nil), _), _, Some("a2")) :: Nil =>
        // success
      case _ => fail(s"Expression after closure translation doesn't match expected, " +
        s"current: ${expressions.mkString(", ")}")
    }
  }

  test("translates scala map function, flatten return value of if expression") {
    val f = (v: A) => if (v.a1 > 0) v else null
    val expressions = ClosureTranslation.translateMap(f, classOf[A], schemaFor[A])
    assert(expressions.length == 2)

    val expected =
      GetStructField(
        AssertNotNull(
        IfExpression(
          "a1".attr <= 0,
          Literal(null)
            .cast(
              StructType(
                Seq(
                  StructField("a1", IntegerType),
                  StructField("a2", IntegerType)))
            ),
          CreateNamedStruct(Seq(Literal("a1"), "a1".attr, Literal("a2"), "a2".attr))
        ),
        Seq("top level non-flat input object")
      ),
      0,
      Some("a1")
    )
    assert(expressions(0) == expected)
  }

  test("handles NPE correctly") {
    val f = (v: B) => v.b1.a1 > 0
    val expression = ClosureTranslation.translateFilter(f, classOf[B], schemaFor[B])
    assertEqual(
      expression,
      Some(GreaterThan(
          NPEOnNull(UnresolvedAttribute("b1"), UnresolvedAttribute("b1.a1")),
          Literal(0)))
    )
  }

  private def schemaFor[T: TypeTag] = {
    ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
  }

  test("translate closure with primitive return type") {
    val f = (v: Int) => v
    val schema = StructType(Seq(StructField("a", IntegerType)))
    val expression = ClosureTranslation.translateMap(f, classOf[Int], schema)
    assert(expression.length == 1)
    assert(expression(0) == UnresolvedAttribute("a" :: Nil))
  }

  test("add AssertNotNull that top level object should not be null") {
    val f = (v: B) => v.b1
    val expressions = ClosureTranslation.translateMap(f, classOf[B], schemaFor[B]).toList
    expressions match {
      case GetStructField(AssertNotNull(UnresolvedAttribute("b1" :: Nil), _), _, Some("a1")) ::
        GetStructField(AssertNotNull(UnresolvedAttribute("b1" :: Nil), _), _, Some("a2")) :: Nil =>
        // success
      case _ => fail(s"Expression after closure translation doesn't match expected, " +
        s"current: ${expressions.mkString(", ")}")
    }
  }

  test("fails to translate the closure if providing a unrecognized schema") {
    val f = (v: A) => v.a1
    val specialSchema = StructType(Seq(StructField("obj", ObjectType(classOf[A]))))
    val expressions = ClosureTranslation.translateMap(f, classOf[A], specialSchema).toList
    // fails to translate the closure
    assert(expressions.length == 0)
  }

  private def assertEqual(a: Any, b: Any): Unit = assert(a == b)
}

object ClosureTranslationSuite {
  case class A(a1: Int, a2: Int)
  case class B(b1: A, b2: A)
}
