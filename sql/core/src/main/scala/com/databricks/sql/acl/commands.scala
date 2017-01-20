/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalog.{Catalog => SparkCatalog}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types._

/**
 * Base trait for an [[AclClient]] related command.
 */
trait AclCommand extends RunnableCommand {
  /** Check if a securable exists in the catalog. */
  protected def qualify(catalog: SparkCatalog, securable: Securable): Option[Securable] = {
    securable match {
      case Catalog =>
        Some(securable)
      case Database(db) if catalog.databaseExists(db) =>
        Some(securable)
      case Table(TableIdentifier(tbl, None)) if catalog.tableExists(tbl) =>
        val qualified = catalog.getTable(tbl)
        Some(Table(TableIdentifier(tbl, Option(qualified.database))))
      case Table(TableIdentifier(tbl, Some(db))) if catalog.tableExists(db, tbl) =>
        Some(securable)
      case AnonymousFunction =>
        Some(securable)
      case Function(FunctionIdentifier(fn, None)) if catalog.functionExists(fn) =>
        val qualified = catalog.getTable(fn)
        Some(Function(FunctionIdentifier(fn, Option(qualified.database))))
      case Function(FunctionIdentifier(fn, Some(db))) if catalog.functionExists(db, fn) =>
        Some(securable)
      case AnyFile =>
        Some(securable)
      case _ => None
    }
  }

  /** Execute a function if the given object exists. */
  protected def mapIfExists[T](
      catalog: SparkCatalog,
      securable: Securable)(f: Securable => T): T = {
    qualify(catalog, securable).map(f).getOrElse {
      throw new SecurityException(s"$securable does not exist.")
    }
  }
}

/**
 * Grant one or more permissions.
 */
case class GrantPermissionsCommand(
    client: AclClient,
    permissions: Seq[Permission])
  extends AclCommand {
  override def run(session: SparkSession): Seq[Row] = {
    client.modify(permissions.map { permission =>
      mapIfExists(session.catalog, permission.securable) { securable =>
        GrantPermission(permission.copy(securable = securable))
      }
    })
    Seq.empty
  }
}

/**
 * Revoke one or more permissions.
 */
case class RevokePermissionsCommand(
    client: AclClient,
    permissions: Seq[Permission])
  extends AclCommand {
  override def run(session: SparkSession): Seq[Row] = {
    client.modify(permissions.map { permission =>
      mapIfExists(session.catalog, permission.securable) { securable =>
        RevokePermission(permission.copy(securable = securable))
      }
    })
    Seq.empty
  }
}

/**
 * Clean dangling permissions on a non-existent securable.
 */
case class CleanPermissionsCommand(
    client: AclClient,
    securable: Securable)
  extends AclCommand {
  require(securable != Catalog, "Cannot clean permissions on the Catalog.")
  override def run(session: SparkSession): Seq[Row] = {
    if (qualify(session.catalog, securable).isEmpty) {
      client.modify(Seq(DropSecurable(securable)))
    }
    Seq.empty
  }
}

/**
 * Change the owner of an object.
 */
case class ChangeOwnerCommand(
    client: AclClient,
    securable: Securable,
    principal: Principal)
  extends AclCommand {
  override def run(session: SparkSession): Seq[Row] = {
    mapIfExists(session.catalog, securable) { qualified =>
      client.modify(Seq(ChangeOwner(qualified, principal)))
    }
    Seq.empty
  }
}

/**
 * Show the permissions for an (optional) principal and or an (optional) securable.
 */
case class ShowPermissionsCommand(
    client: AclClient,
    principal: Option[Principal],
    securable: Securable)
  extends AclCommand {

  override val output: Seq[Attribute] = {
    val attrs = Seq(
      "Principal" -> StringType,
      "ActionType" -> StringType,
      "ObjectType" -> StringType,
      "ObjectKey" -> StringType)
    attrs.map { case (name, dt) =>
      AttributeReference(name, dt, nullable = false, Metadata.empty)()
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val qualified = qualify(sparkSession.catalog, securable).getOrElse {
      throw new SecurityException(s"$securable does not exist.")
    }
    client.listPermissions(principal, qualified).map { p =>
      Row(
        p.principal.name,
        p.action.name,
        p.securable.typeName,
        p.securable.key.toString)
    }
  }
}
