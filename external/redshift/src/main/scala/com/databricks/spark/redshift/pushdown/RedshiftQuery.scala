/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift.pushdown

import com.databricks.spark.redshift.{RedshiftConnectorException, RedshiftRelation}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}

/** Building blocks of a translated query, with nested subqueries. */
private[pushdown] abstract sealed class RedshiftQuery extends LeafNode {

  /** Original plan node from which this RedshiftQuery was build. */
  val origNode: LogicalPlan

  /** Output columns. */
  override lazy val output: Seq[Attribute] =
    if (helper == null) Seq.empty else helper.output

  val helper: QueryHelper

  /** What comes after the FROM clause. */
  val suffix: String = ""

  def expressionToString(expr: Expression): String = {
    ExpressionToSQL.convert(expr, helper.colSet)
  }

  /**
   * Converts this query into a String representing the SQL.
   *
   * @param useAlias Whether or not to alias this translated block of SQL.
   * @return SQL statement for this query.
   */
  def getQuery(useAlias: Boolean = false): String = {
    log.debug(s"""Generating a query of type: ${getClass.getSimpleName}""")

    val query = s"""SELECT ${helper.columns.getOrElse("*")} FROM ${helper.source}$suffix"""

    if (useAlias) {
      block(query, helper.alias)
    } else {
      query
    }
  }

  /**
   * Finds a particular query type in the overall tree.
   *
   * @param query PartialFunction defining a positive result.
   * @tparam T RedshiftQuery type
   * @return Option[T] for one positive match, or None if nothing found.
   */
  def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] = {
    def childrenFind(children: Seq[RedshiftQuery]): Option[T] = children match {
      case Nil => None
      case head :: tail => head.find[T](query).orElse(childrenFind(tail))
    }
    query.lift(this).orElse(childrenFind(helper.children))
  }

  /**
   * Finds a SourceQuery (RedshiftRelation source) in this tree.
   * FIXME: With joins enabled, we will probably be interested in all SourceQueries.
   * @return Some [[SourceQuery]] wrapping a [[RedshiftRelation]]
   *         found in this [[RedshiftQuery]] tree.
   */
  def getSource: SourceQuery = {
    find[SourceQuery] {
      case q: SourceQuery => q
    }.getOrElse(throw new RedshiftConnectorException(
      "Something went wrong: a query tree was generated with no " +
        "Redshift SourceQuery found."))
  }

  /**
   * Determines if two RedshiftQuery subtrees can be joined together.
   *
   * @param otherTree The other tree, can it be joined with this one?
   * @return True if can be joined, or False if not.
   */
  def canJoin(otherTree: RedshiftQuery): Boolean = {
    getSource.cluster == otherTree.getSource.cluster
  }
}

/**
 * The query for a base type (representing a table or view).
 *
 * @param origNode The LogicalPlan it was created from.
 * @param redshiftRelation The base RedshiftRelation representing the basic table, view, or subquery
 *                 defined by the user.
 * @param refColumns Columns used to override the output generation for the QueryHelper.
 *                   These are the columns resolved by RedshiftRelation.
 * @param alias Query alias.
 */
case class SourceQuery(
    origNode: LogicalPlan,
    redshiftRelation: RedshiftRelation,
    refColumns: Seq[Attribute],
    alias: String)
  extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq.empty,
      projections = None,
      outputAttributes = Some(refColumns),
      alias = alias,
      conjunction = redshiftRelation.params.query.map(
          block(_, alias = "redshift_connector_source_query")
        ).getOrElse(redshiftRelation.params.table.get.toString))

  /**
   * Unique identifier of the Redshift cluster that houses this base relation.
   * Currently an exact match on cluster is needed for a join, but we may not need
   * to be this strict.
   * FIXME: Investigate it when joins supported is added.
   */
  val cluster: String = (redshiftRelation.params.jdbcUrl)
}

/**
 * The query for a filter operation.
 *
 * @param origNode The LogicalPlan it was created from.
 * @param conditions The filter condition.
 * @param child The child node.
 * @param alias Query alias.
 */
case class FilterQuery(
    origNode: LogicalPlan,
    conditions: Seq[Expression],
    child: RedshiftQuery,
    alias: String,
    fields: Option[Seq[AttributeReference]] = None)
  extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = None,
      outputAttributes = None,
      alias = alias,
      fields = fields)

  override val suffix = " WHERE " + conditions
      .map(cond => expressionToString(cond))
      .mkString(" AND ")
}

/**
 * The query for a projection operation.
 *
 * @param origNode The LogicalPlan it was created from.
 * @param columns The projection columns.
 * @param child The child node.
 * @param alias Query alias.
 */
case class ProjectQuery(
    origNode: LogicalPlan,
    columns: Seq[NamedExpression],
    child: RedshiftQuery,
    alias: String)
  extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = Some(columns),
      outputAttributes = None,
      alias = alias)
}

/**
 * The query for an aggregation operation.
 *
 * @param origNode The LogicalPlan it was created from.
 * @param columns The projection columns, containing also the aggregate expressions.
 * @param groups The grouping columns.
 * @param child The child node.
 * @param alias Query alias.
 */
case class AggregateQuery(
    origNode: LogicalPlan,
    columns: Seq[NamedExpression],
    groups: Seq[Expression],
    child: RedshiftQuery,
    alias: String)
  extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = Some(columns),
      outputAttributes = None,
      alias = alias)

  override val suffix =
    if (!groups.isEmpty) {
      " GROUP BY " + groups
        .map(group => expressionToString(group))
        .mkString(", ")
    } else {
      ""
    }
}

/**
 * The query for Sort and Limit operations.
 *
 * @param origNode The LogicalPlan it was created from.
 * @param limit Limit expression.
 * @param orderBy Order By expressions.
 * @param child The child node.
 * @param alias Query alias.
 */
case class SortLimitQuery(
    origNode: LogicalPlan,
    limit: Option[Expression],
    orderBy: Seq[Expression],
    child: RedshiftQuery,
    alias: String)
  extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = None,
      outputAttributes = None,
      alias = alias)

  override val suffix = {
    val order_clause =
      if (orderBy.nonEmpty) {
        " ORDER BY " + orderBy.map(e => expressionToString(e)).mkString(", ")
      } else {
        ""
      }

    order_clause +
      limit.map(l => " LIMIT " + expressionToString(l)).getOrElse("")
  }
}

/**
 * The query for join operations.
 *
 * @note Left semi join is treated separately and supported using [[LeftSemiJoinQuery]]
 *
 * @param origNode The LogicalPlan it was created from.
 * @param left The left query subtree.
 * @param right The right query subtree.
 * @param conditions The join conditions.
 * @param joinType The join type.
 * @param alias Query alias.
 */
case class JoinQuery(
    origNode: LogicalPlan,
    left: RedshiftQuery,
    right: RedshiftQuery,
    conditions: Option[Expression],
    joinType: JoinType,
    alias: String)
  extends RedshiftQuery {

  val conj = joinType match {
    case Inner => "INNER JOIN"
    case LeftOuter => "LEFT OUTER JOIN"
    case RightOuter => "RIGHT OUTER JOIN"
    case FullOuter => "OUTER JOIN"
    case _ => throw new MatchError(s"Join type not supported: $joinType")
  }

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left, right),
      projections = Some(left.helper.outputWithQualifier ++ right.helper.outputWithQualifier),
      outputAttributes = None,
      alias = alias,
      conjunction = conj)

  override val suffix = {
    val str = conditions match {
      case Some(e) => " ON "
      case None => ""
    }
    str + conditions.map(cond => expressionToString(cond)).mkString(" AND ")
  }
}

case class LeftSemiJoinQuery(
    origNode: LogicalPlan,
    left: RedshiftQuery,
    right: RedshiftQuery,
    conditions: Option[Expression],
    isAntiJoin: Boolean = false,
    alias: Iterator[String])
  extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left),
      projections = Some(left.helper.outputWithQualifier),
      outputAttributes = None,
      alias = alias.next)

  val cond = if (conditions.isEmpty) Seq.empty else Seq(conditions.get)

  val anti = if (isAntiJoin) " NOT " else " "

  override val suffix = " WHERE" + anti + "EXISTS" + block(
      FilterQuery(
        origNode = origNode,
        conditions = cond,
        child = right,
        alias = alias.next,
        fields = Some(left.helper.outputWithQualifier ++ right.helper.outputWithQualifier))
        .getQuery(useAlias = false))
}
