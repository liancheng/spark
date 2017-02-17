/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift.pushdown

import scala.util.control.NonFatal

import com.databricks.spark.redshift.{Parameters, RedshiftConnectorException, RedshiftRelation}
import com.databricks.sql.DatabricksSQLConf
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Given a `LogicalPlan`, this optimizer rule attempts to push down and wrap as much of query
 * as possible into the leaf `RedshiftRelation`.
 */
private[redshift] case class RedshiftPushdown(session: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (session.conf.get(DatabricksSQLConf.REDSHIFT_ADVANCED_PUSHDOWN.key,
        DatabricksSQLConf.REDSHIFT_ADVANCED_PUSHDOWN.defaultValueString).toBoolean == true) {
      log.debug("Using advanced redshift pushdown infrastructure.")
      RedshiftPushdown.pushdown(plan)
    } else {
      /* Redshift pushdown disabled. */
      plan
    }
  }
}

/**
 * Extractor of a plan child which is a pushed down RedshiftQuery.
 * Needed because sometimes we need to skip over a node.
 */
private[redshift] object PushedRedshiftQuery {
  def unapply(p: LogicalPlan): Option[RedshiftQuery] = p match {
    /* skip over nodes */
    case ReturnAnswer(PushedRedshiftQuery(child)) => Option(child)
    case SubqueryAlias(_, PushedRedshiftQuery(child), _) => Option(child)
    case Project(Nil, PushedRedshiftQuery(child)) => Option(child)
    /* RedshiftQuery */
    case child: RedshiftQuery => Option(child)

    case _ => None
  }
}

private[redshift] object RedshiftPushdown {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Try to push down as much of the query plan into Redshift as we support.
   * It first tries to go bottom up, wrapping up nodes that it can generate Redshift SQL for
   * into [[RedshiftQuery]] helper nodes.
   * Afterwards, it collects all roots of the [[RedshiftQuery]] subtrees, and generates
   * [[RedshiftRelation]] for them with a pushed down Redshift SQL query.
   * @param plan LogicalPlan to be transformed.
   * @return LogicalPlan with as much pushed down bottom-up into Redshift as we support.
   */
  def pushdown(plan: LogicalPlan): LogicalPlan = {
    /** This iterator increments every time it is used and is for aliasing subqueries */
    val alias = Iterator.from(0).map(n => s"subquery_$n")

    plan.transformUp(PartialFunction(p => {
      try {
        /* First try to bottom-up collect what can be pushed down into Redshift. */
        p match {
          /* Redshift source */
          case LogicalRelation(relation: RedshiftRelation, _, _) =>
            SourceQuery(p, relation, p.output, alias.next())

          case Filter(condition, PushedRedshiftQuery(child)) =>
            FilterQuery(p, Seq(condition), child, alias.next())

          case Project(Nil, PushedRedshiftQuery(child)) =>
            /* Note: Project with an empty column list is a special case under a count() Aggr.
             * Don't handle it otherwise. */
            p
          case Project(fields, PushedRedshiftQuery(child)) =>
            ProjectQuery(p, fields, child, alias.next())

          case Limit(limitExpr, PushedRedshiftQuery(child)) =>
            /* WARNING:
             * We rely here on the fact that a query such as:
             * ```SELECT * FROM (SELECT * FROM tbl ORDER BY col) LIMIT 10```
             * will return correct results, even though the SQL Standard doesn't guarantee this.
             * We assume that there is no reason why Redshift would not ensure this in practice.
             * It's actually something they seem to imply with their 'LIMIT' workaround below:
             * http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html#unload-usage-notes */
            SortLimitQuery(p, Some(limitExpr), Seq.empty, child, alias.next())

          case Sort(orderExpr, true, PushedRedshiftQuery(child)) =>
            SortLimitQuery(p, None, orderExpr, child, alias.next())

          // TODO: Gradually re-enable these cases once proper testing is in place.
          //      case l @ Aggregate(groups, fields, child) =>
          //        AggregateQuery(l, fields, groups, child, alias.next())
          //      case l @ Aggregate(groups, fields, Project(Nil, child: RedshiftQuery) =>
          //        note: count() on an empty project needs special handling.
          //        AggregateQuery(l, fields, groups, child, alias.next())) =>

          //      case p @ Join(l: RedshiftQuery, r: RedshiftQuery, joinType, condition) =>
          //        joinType match {
          //          case Inner | LeftOuter | RightOuter | FullOuter =>
          //            JoinQuery(p, l, r, condition, joinType, alias.next())
          //          case LeftSemi =>
          //            LeftSemiJoinQuery(p, l, r, condition, false, alias)
          //          case LeftAnti =>
          //            LeftSemiJoinQuery(p, l, r, condition, true, alias)
          //          case _ => p // unhandled pushdown
          //        }

          case _ =>
            if (p.children.forall { case PushedRedshiftQuery(_) => true; case _ => false } ) {
              /* Print debug message at first unhandled node */
              log.debug("Unhandled logical plan node: " + p)
            }
            p
        }
      } catch {
        case ex: RedshiftConnectorException =>
          log.warn("Redshift connector pushdown internal error in plan node: " + p)
          log.warn(ex.toString)
          ex.getStackTrace.foreach(s => log.warn(s.toString))
          p
        case NonFatal(ex) =>
          log.debug("Unhandled expression inside logical plan node: " + p)
          log.debug(ex.toString())
          ex.getStackTrace.take(5).foreach(s => log.debug(s.toString))
          ex.getStackTrace.drop(5).foreach(s => log.trace(s.toString))
          p
      }
    })).transform {
      /* Then transform all pushed down redshift queries
         into RedshiftRelations with "query" parameter. */

      /* Avoid infinite recursion if there is nothing sensible to push down. */
      case pushdownRoot: SourceQuery =>
        /* Nothing more than source RedshiftRelation pushed down, put it back. */
        pushdownRoot.origNode
      case pushdownRoot @ ProjectQuery(_, cols, child: SourceQuery, _)
          if cols.forall {
            case Alias(_ : Attribute, _) => true
            case _ => false
          } =>
        /* Nothing more than a Project with aliases pushed down, put it back. */
        pushdownRoot.origNode

      case pushdownRoot: RedshiftQuery =>
        try {
          /* Find RedshiftRelation in the pushed down RedshiftQuery. */
          val redshiftRelation = pushdownRoot.getSource.redshiftRelation
          /* Generate the SQL query for Redshift.
           * Note: Due to a very specific Redshift quirk, LIMIT clauses are not supported in
           * the outermost SELECT query within an UNLOAD command. See below:
           * http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html#unload-usage-notes
           * However, the SQL query generated here will be wrapped in a subquery in
           * RedshiftRelation.buildStandardQuery */
          val sqlQuery = pushdownRoot.getQuery()
          /* Check if the output is supposed to be sorted
           * - in that case, Redshift has to unload it into one file. */
          val sortedOutput = pushdownRoot match {
            case SortLimitQuery(_, _, orderBy, _, _) if (orderBy.nonEmpty) => true
            case _ => false
          }
          /* New parameters for the RedshiftRelation
           * - remove "dbtable" if it was originally scanning a table.
           * - replace "query" with the generated sql query.
           * - specify if the output is supposed to be sorted.
           */
          val newParams = Parameters.mergeParameters(
            redshiftRelation.params.parameters
              - ("dbtable") // remove dbtable
              + ("query" -> sqlQuery) // add generated query
              + ("singleoutput" -> sortedOutput.toString) // specify output sortedness
          )
          /* New schema for the RedshiftRelation */
          val newSchema = Option(StructType(
            pushdownRoot.output.map(
              attr => StructField(attr.name, attr.dataType, attr.nullable))))

          /* New LogicalRelation with the pushed down query.
           * Reuse the original RedshiftRelation, with new parameters and schema. */
          val pushdownRel = LogicalRelation(
            redshiftRelation.copy(
              params = newParams, userSchema = newSchema)(redshiftRelation.sqlContext),
            Option(pushdownRoot.output),
            None)
          /* Project all the column names back to the original column names. */
          if (pushdownRoot.output.length != pushdownRoot.origNode.output.length &&
              !(pushdownRoot.origNode.output.length == 0) /* special case for empty Project */) {
            throw new RedshiftConnectorException(
              "Pushed down query has different number of output columns than original query.");
          }
          val projectBack = (pushdownRoot.output zip pushdownRoot.origNode.output).map {
            case (renamed, orig) =>
              if (renamed.dataType != orig.dataType) {
                throw new RedshiftConnectorException(
                  "Pushed down query column has different datatype than in original query.");
              }
              Alias(renamed, orig.name)(orig.exprId, orig.qualifier, Option(orig.metadata))
          }
          Project(projectBack, pushdownRel)
        } catch {
          case ex: RedshiftConnectorException =>
            log.warn("Redshift connector pushdown internal error in constructing: " + pushdownRoot)
            log.warn(ex.toString)
            ex.getStackTrace.foreach(s => log.warn(s.toString))
            pushdownRoot.origNode
        }
    }
  }
}
