/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.parser

import scala.collection.JavaConverters._

import com.databricks.sql.parser.DatabricksSqlBaseParser._
import com.databricks.sql.transaction.VacuumTableCommand

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Build a Databricks-specific [[LogicalPlan]] from an ANTLR4 parser tree
 */
class DatabricksSqlCommandBuilder
  extends DatabricksSqlBaseBaseVisitor[AnyRef] {
  import ParserUtils._

  /**
   * Entry point for the parsing process.
   */
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  /**
   * Create a [[TableIdentifier]] from a qualified name.
   */
  protected def visitTableIdentifier(ctx: QualifiedNameContext): TableIdentifier = withOrigin(ctx) {
    ctx.identifier.asScala match {
      case Seq(tbl) => TableIdentifier(tbl.getText)
      case Seq(db, tbl) => TableIdentifier(tbl.getText, Some(db.getText))
      case _ => throw new ParseException(s"Illegal table name ${ctx.getText}", ctx)
    }
  }

  /**
   * Create a [[FunctionIdentifier]] from a qualified name.
   */
  protected def visitFunctionIdentifier(ctx: QualifiedNameContext): FunctionIdentifier = {
    withOrigin(ctx) {
      ctx.identifier.asScala match {
        case Seq(tbl) => FunctionIdentifier(tbl.getText)
        case Seq(db, tbl) => FunctionIdentifier(tbl.getText, Some(db.getText))
        case _ => throw new ParseException(s"Illegal function name ${ctx.getText}", ctx)
      }
    }
  }

  /**
   * Create a [[VacuumTable]] logical plan.
   * Example SQL :
   * {{{
   *   VACUUM ('/path/to/dir' | table_name) [RETAIN number HOURS];
   * }}}
   */
  override def visitVacuumTable(
    ctx: VacuumTableContext): LogicalPlan = withOrigin(ctx) {
    VacuumTableCommand(
      Option(ctx.path).map(string),
      Option(ctx.table).map(visitTableIdentifier),
      Option(ctx.number).map(_.getText.toDouble))
  }

  /**
   * Return null for every other query. These queries should be passed to a delegate parser.
   */
  override def visitPassThrough(ctx: PassThroughContext): LogicalPlan = null
}
