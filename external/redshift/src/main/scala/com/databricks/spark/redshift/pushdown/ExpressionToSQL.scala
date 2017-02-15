/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift.pushdown

import com.databricks.spark.redshift.Utils

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

/**
 * Converter class to be used for turning a Catalyst `Expression` into a piece of SQL text
 * that Redshift can digest.
 *
 * @param fields a set of "context" attributes to be used when resolving `AttributeReference`s
 */
private[pushdown] class ExpressionToSQL(fields: Seq[Attribute]) {
  type ExpressionToSqlPF = PartialFunction[Expression, String]

  private val literalExprs: ExpressionToSqlPF = {
    case v @ Literal(_, StringType) => s"'${Utils.escapeRedshiftStringLiteral(v.toString())}'"
    case v @ Literal(_, BooleanType) => v.toString()
    case v @ Literal(_, _: NumericType) => v.toString()
    // Let's be safe and not try to handle anything else right now.
    // Dates and Timestamps are always troublesome...
  }

  private val basicExprs: ExpressionToSqlPF = {
    case a: AttributeReference =>
      addAttribute(a, fields)

    case Alias(child: Expression, name: String) =>
      block(convertExpression(child), name)

    case SortOrder(child, direction, nullOrdering) =>
      s"${block(convertExpression(child))} ${direction.sql} ${nullOrdering.sql}"
  }

  private val booleanExprs: ExpressionToSqlPF = {
    case In(child, list) if list.forall(_.isInstanceOf[Literal]) =>
      block(convertExpression(child) + " IN " + block(convertExpressions(list)))

    case IsNull(child) =>
      block(convertExpression(child) + " IS NULL")
    case IsNotNull(child) =>
      block(convertExpression(child) + " IS NOT NULL")

    case Not(child) =>
      "NOT" + block(convertExpression(child))
  }

  private val binaryExprs: ExpressionToSqlPF = {
    case expr @ (_: And | _: Or |
        _: LessThan | _: LessThanOrEqual | _: EqualTo | _: GreaterThan | _: GreaterThanOrEqual |
        _: Add | _: Subtract | _: Multiply | _: Divide) if expr.isInstanceOf[BinaryOperator] =>

      val bo = expr.asInstanceOf[BinaryOperator] // checked above

      val leftSQL = convertExpression(bo.left)
      val rightSQL = convertExpression(bo.right)
      val opSQL = bo.sqlOperator

      block(s"$leftSQL $opSQL $rightSQL")
  }

  private val stringExprs: ExpressionToSqlPF = {
    case expr @ (_: Ascii | _: Lower | _: Substring | _: StringLPad | _: StringRPad |
        _: StringReverse | _: StringTranslate | _: StringTrim |
        _: StringTrimLeft | _: StringTrimRight | _: Upper) =>
      expr.prettyName.toUpperCase + block(convertExpressions(expr.children))

    case Concat(children) if children.length == 2 =>
      "CONCAT" + block(convertExpressions(children))

    case expr @ (_: Like | _: StartsWith | _: Contains | _: EndsWith) =>
      val (attr: Expression, pattern: String) = expr match {
        case Like(attr, pattern @ Literal(_, StringType)) =>
          (attr, pattern.toString)
        case StartsWith(attr, Literal(pattern, StringType)) =>
          (attr, pattern + "%")
        case EndsWith(attr, Literal(pattern, StringType)) =>
          (attr, "%" + pattern)
        case Contains(attr, Literal(pattern, StringType)) =>
          (attr, "%" + pattern + "%")
      }
      block(convertExpression(attr) + " LIKE " + convertExpression(Literal(pattern)))
  }

  /* Currently untested and unused */
  private val numericExpression: ExpressionToSqlPF = {
    case expr @ (_: Abs | _: Acos | _: Cos | _: Tan | _: Tanh | _: Cosh | _: Atan |
        _: Floor | _: Sin | _: Log | _: Asin | _: Sqrt | _: Ceil | _: Sqrt | _: Sinh |
        _: Greatest | _: Least) =>
      expr.prettyName.toUpperCase + block(convertExpressions(expr.children))

    case CheckOverflow(child, _) => convertExpression(child)

    case Pi() => "PI()"

    case Rand(seed) => "RANDOM" + block(seed.toString)
    case Round(child, scale) =>
      "ROUND" + block(convertExpressions(Seq(child, scale)))
  }

  /* Currently untested and unused */
  private val dateExprs: ExpressionToSqlPF = {
    case expr @ (_: Month | _: Quarter | _: Year) =>
      expr.prettyName.toUpperCase + block(convertExpressions(expr.children))
  }

  /* Currently untested and unused */
  private val aggrExprs: ExpressionToSqlPF = {
    case AggregateExpression(
        aggFcn @ (
          _: Average | _: Corr | _: CovPopulation | _: CovSample | _: Count | _: Max | _: Min |
          _: Sum | _: StddevPop | _: StddevSamp | _: VariancePop | _: VarianceSamp
        ), _, isDistinct, _) =>
      val distinct: String = if (isDistinct) "DISTINCT " else ""
      aggFcn.prettyName.toUpperCase + block(distinct + convertExpressions(aggFcn.children))
  }

  private val miscExprs: ExpressionToSqlPF = {
    case Cast(child, t) if getCastType(t).isDefined =>
      val cast = getCastType(t).get
      "CAST" + block(convertExpression(child) + " AS " + cast)

//    case e @ ScalarSubquery(subquery, _, _) =>
//      RedshiftPushdown.pushdown(subquery) match {
//        case p: RedshiftQuery => block(p.getQuery())
//        case _ => throw new MatchError(e)
//      }
//
//    case If(child, trueValue, falseValue) =>
//      "IFF" + block(convertExpressions(fields, child, trueValue, falseValue))
//
//    case CaseWhenCodegen(branches, elseValue) =>
//      val cases = "CASE " + branches
//          .map(
//            b =>
//              "WHEN " + convertExpression(b._1) + " THEN " + convertExpression(
//                b._2,
//                fields))
//          .mkString(" ")
//      if (elseValue.isDefined)
//        block(
//          cases + " ELSE " + convertExpression(elseValue.get) + " END")
//      else block(cases + " END")
}

  /**
   * Attempts a best effort conversion from a SparkType
   * to a Redshift type to be used in a Cast.
   */
  private final def getCastType(t: DataType): Option[String] =
    Option(t match {
      case StringType => "VARCHAR"
      case BooleanType => "BOOLEAN"
      case d: DecimalType => "DECIMAL(" + d.precision + ", " + d.scale + ")"
      case IntegerType => "INT4"
      case LongType => "INT8"
      case FloatType => "FLOAT4"
      case DoubleType => "FLOAT8"
//      case BinaryType => "BINARY"
//      case DateType => "DATE"
//      case TimestampType => "TIMESTAMP"
      case _ => null
    })


  /**
   * Recursive function that converts Spark expressions to SQL statements runnable by Redshift.
   *
   * @note A MatchError will be raised if we encounter an expression that we don't (yet) support.
   *
   * @note As a consequence of the above, but also by design, a complex expression is either pushed
   *       down as a whole, or not at all. One may imagine we could break down AND lists of
   *       predicates into supported and unsupported expressions and do partial push-down, but we
   *       currently don't.
   */
  final def convertExpression(expr: Expression): String = {
    literalExprs
      .orElse(basicExprs)
      .orElse(booleanExprs)
      .orElse(binaryExprs)
      .orElse(stringExprs)
//      .orElse(numericExprs)
//      .orElse(dateExprs)
//      .orElse(aggrExprs)
      .orElse(miscExprs)
      .applyOrElse(expr, { expr: Expression => throw new MatchError(expr) })
  }

  /**
   * Calls `convertExpression` on every input expression and connects the results with ", ".
   */
  final def convertExpressions(expressions: Seq[Expression]): String = {
    expressions.map(e => convertExpression(e)).mkString(", ")
  }
}

private[pushdown] object ExpressionToSQL {
  /**
   * Helper function that instantiates an `ExpressionToSQL` converter object
   * and calls its `convertExpression()` method with the provided arguments.
   */
  def convert(expr: Expression, fields: Seq[Attribute]): String = {
    val converter = new ExpressionToSQL(fields)
    converter.convertExpression(expr)
  }
}
