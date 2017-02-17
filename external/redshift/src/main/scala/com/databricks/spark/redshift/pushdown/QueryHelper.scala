/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift.pushdown

import com.databricks.spark.redshift.RedshiftConnectorException

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}

/**
 * Helper class to maintain the fields, output, and projection expressions of
 * a RedshiftQuery. This may be refactored into the RedshiftQuery abstract class.
 *
 * @constructor Creates an instance of a QueryHelper. Created by every RedshiftQuery.
 * @param children A sequence containing the child queries. May be empty in the case
 *                 of a source (bottom-level) query, contain one element (for most
 *                 unary operations), or contain two elements (for joins, etc.).
 * @param projections Contains optional projection columns for this query.
 * @param outputAttributes Optional manual override for output.
 * @param alias The alias for this subquery.
 * @param conjunction Conjunction phrase to be used in between subquery children, or simple phrase
 *                    when there are no subqueries.
 */
private[pushdown] case class QueryHelper(
    children: Seq[RedshiftQuery],
    projections: Option[Seq[NamedExpression]] = None,
    outputAttributes: Option[Seq[Attribute]],
    alias: String,
    conjunction: String = "",
    fields: Option[Seq[AttributeReference]] = None) {

  /** Set of input columns with qualifiers. */
  val colSet =
    fields.getOrElse {
      children.flatMap(_.helper.outputWithQualifier)
    }

  /** Set of input columns without qualifiers. */
  val pureColSet: Seq[Attribute] =
    children.flatMap(_.helper.output)

  /**
   * (Optional) projections of this operator.
   * Processed to bind names to reference input columns,
   * and to rename everything to new aliases to simplify avoiding conflicts in generated SQL.
   */
  val processedProjections: Option[Seq[NamedExpression]] = projections
    .map(p => // if projections not None
      p.map(e => // for each NamedExpression in projections
        colSet.find(c => c.exprId == e.exprId) match { // find the NamedExpression in input cols
          case Some(a) => // and replace with AttributeReference if found.
            AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, None)
          case None => e
      }))
    .map(p => renameColumns(p, alias)) // and then always rename columns to new aliases.

  /** (Optional) output columns as a SQL string. */
  val columns: Option[String] = processedProjections map { p =>
    p.map(e => ExpressionToSQL.convert(e, colSet)).mkString(", ")
  }

  /** Output columns. */
  val output: Seq[Attribute] = {
    processedProjections.map(p => p.map(_.toAttribute)).getOrElse {

      if (children.isEmpty) {
        outputAttributes.getOrElse(throw new RedshiftConnectorException(
          "Query output attributes must not be empty when it has no children."))
      } else {
        children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output)
      }
    }
  }

  /** Output colums with qualifiers. */
  val outputWithQualifier: Seq[AttributeReference] = output.map(
    a => AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, Some(alias)))

  /** SQL string generated for the children. */
  val source: String =
    if (children.nonEmpty) {
      children.map(c => c.getQuery(useAlias = true)).mkString(s""" $conjunction """)
    } else {
      conjunction
    }
}
