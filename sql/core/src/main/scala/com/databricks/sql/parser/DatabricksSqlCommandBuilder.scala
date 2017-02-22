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

import com.databricks.sql.acl._
import com.databricks.sql.parser.DatabricksSqlBaseParser._
import com.databricks.sql.transaction.VacuumTableCommand

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Build an ACL related [[LogicalPlan]] from an ANTLR4 parser tree
 */
class DatabricksSqlCommandBuilder(client: AclClient)
  extends DatabricksSqlBaseBaseVisitor[AnyRef] {
  import ParserUtils._

  /**
   * Entry point for the parsing process.
   */
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  /**
   * Create a Grant/Revoke permission managing command, returning either a
   * [[GrantPermissionsCommand]] or a [[RevokePermissionsCommand]].
   *
   * Expected GRANT format:
   * {{{
   *   GRANT [privilege [',' privilege] ... | ALL PRIVILEGES]
   *   ON [objectType] objectName TO principal
   *   [WITH GRANT OPTION]
   * }}}
   *
   * Expected REVOKE format:
   * {{{
   *   REVOKE [GRANT OPTION FOR]
   *   [privilege [',' privilege] ... | ALL PRIVILEGES]
   *   ON [objectType] objectName FROM principal
   * }}}
   */
  override def visitManagePermissions(ctx: ManagePermissionsContext): LogicalPlan = {
    withOrigin(ctx) {
      if (ctx.OPTION != null) {
        throw new ParseException("GRANT OPTION is unsupported. Only the owner of a " +
          "securable may grant permissions", ctx)
      }
      // Create the permission objects.
      val permissions = try {
        val principal = NamedPrincipal(ctx.grantee.getText)
        val securable = visitSecurable(ctx.securable)
        val actions = if (ctx.ALL != null) {
          Action.allRegularActions.filter(_.validateGrant(securable))
        } else {
          ctx.actionTypes.asScala.map(a => Action.fromString(a.getText))
        }
        actions.map(Permission(principal, _, securable))
      } catch {
        case e: IllegalArgumentException =>
          throw new ParseException(e.getMessage, ctx)
      }

      // Create the command.
      if (ctx.REVOKE != null) {
        RevokePermissionsCommand(client, permissions)
      } else {
        GrantPermissionsCommand(client, permissions)
      }
    }
  }

  /**
   * Create permissions listing command. This returns a [[ShowPermissionsCommand]] command.
   *
   * Example SQL:
   * {{{
   *   SHOW GRANT [prinicipal] ON [object]
   * }}}
   */
  override def visitShowPermissions(ctx: ShowPermissionsContext): LogicalPlan = withOrigin(ctx) {
    validate(ctx.securable != null, "A valid securable must be specified", ctx)
    ShowPermissionsCommand(
      client,
      Option(ctx.identifier).map(_.getText).map(NamedPrincipal),
      visitSecurable(ctx.securable))
  }

  /**
   * Alter the owner of a table. This returns a [[ChangeOwnerCommand]].
   *
   * Example SQL:
   * {{{
   *   ALTER [object] SET OWNER TO [principal]
   * }}}
   */
  override def visitAlterOwner(ctx: AlterOwnerContext): LogicalPlan = withOrigin(ctx) {
    ChangeOwnerCommand(
      client,
      visitSecurable(ctx.securable),
      NamedPrincipal(ctx.identifier.getText))
  }

  /**
   * Repair a securable object that has dangling ACLs. This returns a [[CleanPermissionsCommand]]
   * command.
   *
   * Example SQL:
   * {{{
   *   MSCK REPAIR [object] PRIVILEGES
   * }}}
   */
  override def visitRepairPrivileges(ctx: RepairPrivilegesContext): LogicalPlan = {
    withOrigin(ctx) {
      CleanPermissionsCommand(client, visitSecurable(ctx.securable))
    }
  }

  /**
   * Create a [[Securable]] object.
   */
  override def visitSecurable(ctx: SecurableContext): Securable = withOrigin(ctx) {
    Option(ctx.objectType).map(_.getType).getOrElse(DatabricksSqlBaseParser.TABLE) match {
      case DatabricksSqlBaseParser.CATALOG =>
        Catalog
      case DatabricksSqlBaseParser.DATABASE =>
        Database(ctx.identifier.getText)
      case DatabricksSqlBaseParser.VIEW | DatabricksSqlBaseParser.TABLE =>
        Table(visitTableIdentifier(ctx.qualifiedName))
      case DatabricksSqlBaseParser.FUNCTION if ctx.ANONYMOUS != null =>
        AnonymousFunction
      case DatabricksSqlBaseParser.FUNCTION =>
        Function(visitFunctionIdentifier(ctx.qualifiedName))
      case DatabricksSqlBaseParser.FILE =>
        AnyFile
      case _ =>
        throw new ParseException("Unknown Securable Object", ctx)
    }
  }

  /**
   * Create a [[TableIdentifier]] from a qualified name.
   */
  private def visitTableIdentifier(ctx: QualifiedNameContext): TableIdentifier = withOrigin(ctx) {
    ctx.identifier.asScala match {
      case Seq(tbl) => TableIdentifier(tbl.getText)
      case Seq(db, tbl) => TableIdentifier(tbl.getText, Some(db.getText))
      case _ => throw new ParseException(s"Illegal table name ${ctx.getText}", ctx)
    }
  }

  /**
   * Create a [[FunctionIdentifier]] from a qualified name.
   */
  private def visitFunctionIdentifier(ctx: QualifiedNameContext): FunctionIdentifier = {
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
