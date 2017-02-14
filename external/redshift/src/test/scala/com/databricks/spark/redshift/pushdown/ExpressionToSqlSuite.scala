/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift.pushdown

import java.sql.{Date, Timestamp}

import scala.util.control.NonFatal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


class ExpressionToSqlSuite extends SparkFunSuite {
  def testExpr(expr: Expression, fields: Seq[Attribute], expected: String): Unit =
    assertResult(expected)(ExpressionToSQL.convert(expr, fields))

  def testExpr(expr: Expression, expected: String): Unit =
    testExpr(expr, Seq.empty, expected)

  def testExpr(expr: Expression, fields: Seq[Attribute], isValid: String => Boolean): Unit =
    assert(isValid(ExpressionToSQL.convert(expr, fields)))

  def testExpr(expr: Expression, isValid: String => Boolean): Unit =
    testExpr(expr, Seq.empty, isValid)

  def testUnsupportedExpr(expr: Expression, fields: Seq[Attribute] = Seq.empty): Unit =
    try {
      val res = ExpressionToSQL.convert(expr, fields)
      fail(s"Conversion of $expr with $fields returned $res but was expected to fail.")
    } catch {
      case NonFatal(ex) => assert(ex.isInstanceOf[MatchError])
    }


  private def lit(v: Any) = Literal(v)

  private def attr(name: String,
      qual: String = null,
      dataType: DataType = StringType,
      id: Long = 0,
      isGenerated: Boolean = false) =
    AttributeReference(name, dataType)(ExprId(id), Option(qual), isGenerated)


  private def fields: Seq[Attribute] = Seq(
    attr("field1", "qual1", id = 10),
    attr("field2", "qual2", id = 20),
    attr("field3", "qual3", id = 30)
  )

  test("Literals") {
    /* Supported */
    testExpr(lit(true), "true")
    testExpr(lit(false), "false")

    for { t <- Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType) } {
      testExpr(Literal(0, t), "0")
    }
    testExpr(lit(1), "1")
    testExpr(lit(2L), "2")
    testExpr(lit(3.14), "3.14")
    testExpr(lit(-1.0/3.0), _.startsWith("-0.3333"))
    testExpr(lit(Decimal(3.1415, 4, 2)), "3.14")
    testExpr(lit(BigDecimal(9000)), "9000")
    testExpr(lit(1e22), "1.0E22")

    testExpr(lit("qwerty123"), "'qwerty123'")
    // scalastyle:off
    testExpr(lit("They say: '樂趣' \\"), "'They say: ''樂趣'' \\\\'")
    // scalastyle:on

    /* Unsupported */
    testUnsupportedExpr(lit(new Timestamp(123)))
    testUnsupportedExpr(lit(new Date(123)))
    testUnsupportedExpr(lit(Array(1, 2, 3)))
    testUnsupportedExpr(lit(null))
  }

  test("Basic Expressions") {
    /* Supported: */
    testExpr(attr("col"), "\"col\"")
    testExpr(attr("col", "tbl"), "\"tbl\".\"col\"")

    testExpr(Alias(lit(1), "constant")(), "(1) AS \"constant\"")
    // Note that we discard Alias qualifiers.
    testExpr(Alias(attr("col"), "alias")(qualifier = Some("qual")), "(\"col\") AS \"alias\"")

    testExpr(SortOrder(attr("col"), Ascending), "(\"col\") ASC NULLS FIRST")
    testExpr(SortOrder(attr("col"), Ascending, NullsFirst), "(\"col\") ASC NULLS FIRST")
    testExpr(SortOrder(attr("col"), Ascending, NullsLast), "(\"col\") ASC NULLS LAST")
    testExpr(SortOrder(attr("col"), Descending, NullsLast), "(\"col\") DESC NULLS LAST")
    testExpr(SortOrder(attr("col"), Descending, NullsFirst), "(\"col\") DESC NULLS FIRST")
    testExpr(SortOrder(attr("col"), Descending), "(\"col\") DESC NULLS LAST")

    testExpr(attr("attr", id = 20), fields, "\"qual2\".\"field2\"")
    testExpr(attr("attr", id = 50), fields, "\"attr\"")

    /* Unsupported */
    testUnsupportedExpr(PrettyAttribute("x", StringType))
    testUnsupportedExpr(UnresolvedAttribute("table.col"))
  }

  test("Boolean Expressions") {
    /* Supported: */
    testExpr(Not(lit(true)), "NOT(true)")
    testExpr(IsNull(attr("x")), "(\"x\" IS NULL)")
    testExpr(IsNotNull(attr("y")), "(\"y\" IS NOT NULL)")
    testExpr(In(attr("z"), Seq(lit(-1), lit(0), lit(1))), "(\"z\" IN (-1, 0, 1))")

    /* Unsupported */
    testUnsupportedExpr(InSet(attr("z"), Set(-1, 0, 1)))
  }

  test("Binary Expressions") {
    /* Supported */
    testExpr(And(lit(true), lit(false)), "(true AND false)")
    testExpr(Or(attr("x"), attr("y")), "(\"x\" OR \"y\")")

    testExpr(Add(lit(1), lit(1)), "(1 + 1)")
    testExpr(Subtract(lit(1), lit(1)), "(1 - 1)")
    testExpr(Multiply(lit(1), lit(1)), "(1 * 1)")
    testExpr(Divide(lit(1), lit(1)), "(1 / 1)")

    testExpr(LessThan(lit(1), lit(1)), "(1 < 1)")
    testExpr(LessThanOrEqual(lit(1), lit(1)), "(1 <= 1)")
    testExpr(EqualTo(lit(1), lit(1)), "(1 = 1)")
    testExpr(GreaterThanOrEqual(lit(1), lit(1)), "(1 >= 1)")
    testExpr(GreaterThan(lit(1), lit(1)), "(1 > 1)")

    testExpr(
      Or(EqualTo(Add(lit(1), Multiply(lit(2), lit(3))), lit(7)), attr("is it?")),
      "(((1 + (2 * 3)) = 7) OR \"is it?\")"
    )

    /* Unsupported */
    testUnsupportedExpr(Pmod(lit(1), lit(1)))
    testUnsupportedExpr(Remainder(lit(1), lit(1)))
  }

  test("String Expressions") {
    /* Supported */
    testExpr(Lower(attr("string")), "LOWER(\"string\")")
    testExpr(Upper(attr("string")), "UPPER(\"string\")")
    testExpr(Ascii(attr("string")), "ASCII(\"string\")")

    testExpr(StringReverse(attr("string")), "REVERSE(\"string\")")

    testExpr(StringTrim(attr("string")), "TRIM(\"string\")")
    testExpr(StringTrimLeft(attr("string")), "LTRIM(\"string\")")
    testExpr(StringTrimRight(attr("string")), "RTRIM(\"string\")")

    testExpr(Like(attr("string"), lit("p%ttern")), "(\"string\" LIKE 'p%ttern')")
    testExpr(StartsWith(attr("string"), lit("pattern")), "(\"string\" LIKE 'pattern%')")
    testExpr(Contains(attr("string"), lit("pattern")), "(\"string\" LIKE '%pattern%')")
    testExpr(EndsWith(attr("string"), lit("pattern")), "(\"string\" LIKE '%pattern')")

    testExpr(Substring(attr("string"), lit(1), lit(3)), "SUBSTRING(\"string\", 1, 3)")
    testExpr(StringLPad(attr("string"), lit(1), lit(3)), "LPAD(\"string\", 1, 3)")
    testExpr(StringRPad(attr("string"), lit(1), lit(3)), "RPAD(\"string\", 1, 3)")

    testExpr(Concat(Seq(lit("a"), lit("z"))), "CONCAT('a', 'z')")
    testExpr(
      Concat(Seq(attr("c1"), Concat(Seq(lit(", "), attr("c2"))))),
      """CONCAT("c1", CONCAT(', ', "c2"))"""
    )

    /* Unsupported */
    testUnsupportedExpr(Concat(Seq(lit("a"), lit("b"), lit("c"))))
    testUnsupportedExpr(ParseUrl(Seq(lit("a"), lit("b"), lit("c"))))
  }

  test("Misc Expressions") {
    /* Supported */
    testExpr(Cast(attr("x"), StringType), "CAST(\"x\" AS VARCHAR)")
    testExpr(Cast(attr("x"), BooleanType), "CAST(\"x\" AS BOOLEAN)")
    testExpr(Cast(attr("x"), DecimalType(4, 2)), "CAST(\"x\" AS DECIMAL(4, 2))")
    testExpr(Cast(attr("x"), IntegerType), "CAST(\"x\" AS INT4)")
    testExpr(Cast(attr("x"), LongType), "CAST(\"x\" AS INT8)")
    testExpr(Cast(attr("x"), FloatType), "CAST(\"x\" AS FLOAT4)")
    testExpr(Cast(attr("x"), DoubleType), "CAST(\"x\" AS FLOAT8)")


    /* Unsupported */
    testUnsupportedExpr(Cast(attr("x"), BinaryType))
    testUnsupportedExpr(Cast(attr("x"), DateType))
    testUnsupportedExpr(Cast(attr("x"), TimestampType))
  }
}
