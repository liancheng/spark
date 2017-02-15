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

import com.databricks.spark.redshift.Utils

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Helper methods for pushing filters into Redshift queries.
 */
private[redshift] object FilterPushdown {
  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   * @param schema the schema of the table being queried
   * @param filters an array of filters, the conjunction of which is the filter condition for the
   *                scan.
   */
  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }

  /**
   * Attempt to convert the given filter into a SQL expression. Returns None if the expression
   * could not be converted.
   */
  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {

    // Builds an escaped value, based on the expected datatype
    def buildValueWithType(dataType: DataType, value: Any): String = {
      dataType match {
        case StringType => s"'${Utils.escapeRedshiftStringLiteral(value.toString)}'"
        case DateType => s"'${value.asInstanceOf[Date]}'"
        case TimestampType => s"'${value.asInstanceOf[Timestamp]}'"
        case _ => value.toString
      }
    }

    // Builds an escaped value, based on the value itself
    def buildValue(value: Any): String = {
      value match {
        case _: String => s"'${Utils.escapeRedshiftStringLiteral(value.toString)}'"
        case _: Date => s"'${value.asInstanceOf[Date]}'"
        case _: Timestamp => s"'${value.asInstanceOf[Timestamp]}'"
        case _ => value.toString
      }
    }

    // Builds a simple comparison string
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
      for {
        dataType <- getTypeForAttribute(schema, attr)
        sqlEscapedValue = buildValueWithType(dataType, value)
      } yield {
        s"""${wrap(attr)} $comparisonOp $sqlEscapedValue"""
      }
    }

    // Builds a string out of a binary logical operation
    def buildBooleanLogicExpr(left: Filter, right: Filter, logicalOp: String) : Option[String] = {
      for {
        leftStr <- buildFilterExpression(schema, left)
        rightStr <- buildFilterExpression(schema, right)
      } yield {
        s"""$leftStr $logicalOp $rightStr"""
      }
    }

    val predicateOption = filter match {
      case EqualTo(attr, value) =>
        buildComparison(attr, value, "=")
      case LessThan(attr, value) =>
        buildComparison(attr, value, "<")
      case GreaterThan(attr, value) =>
        buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) =>
        buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) =>
        buildComparison(attr, value, ">=")
      case In(attr, values: Array[Any]) =>
        val dataType = getTypeForAttribute(schema, attr).get
        val valueStrings = values.map(v => buildValueWithType(dataType, v)).mkString(", ")
        Some(s"""${wrap(attr)} IN ${block(valueStrings)}""")
      case IsNull(attr) =>
        Some(s"""${wrap(attr)} IS NULL""")
      case IsNotNull(attr) =>
        Some(s"""${wrap(attr)} IS NOT NULL""")
      case And(left, right) =>
        buildBooleanLogicExpr(left, right, "AND")
      case Or(left, right) =>
        buildBooleanLogicExpr(left, right, "OR")
      case Not(child) =>
        buildFilterExpression(schema, child).map(s => s"""NOT $s""")
      case StringStartsWith(attr, value) =>
        Some(s"""${wrap(attr)} LIKE ${buildValue(value + "%")}""")
      case StringEndsWith(attr, value) =>
        Some(s"""${wrap(attr)} LIKE ${buildValue("%" + value)}""")
      case StringContains(attr, value) =>
        Some(s"""${wrap(attr)} LIKE ${buildValue("%" + value + "%")}""")
      case _ => None
    }

    // Let's be safe and wrap every individual expression in parentheses in order to avoid having
    // to reason about operator precedence rules in Redshift, which are briefly documented here:
    // http://docs.aws.amazon.com/redshift/latest/dg/r_logical_condition.html
    // Note that there's no mention of operators such as LIKE, IN, IS NULL, etc.
    predicateOption.map(block)
  }

  /**
   * Use the given schema to look up the attribute's data type. Returns None if the attribute could
   * not be resolved.
   */
  private def getTypeForAttribute(schema: StructType, attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
}
