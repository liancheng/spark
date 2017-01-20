/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._

/**
 * This rule resolves the catalog objects (Databases, Tables & Functions) used in Spark Drop
 * [[org.apache.spark.sql.catalyst.plans.logical.Command]]s. We will throw exceptions when the
 * resolved object does not exist, or drop the command by replacing it with a
 * [[NoOpRunnableCommand]] when the user specifies (if exists).
 */
class ResolveDropCommand(spark: SparkSession) extends Rule[LogicalPlan] {
  private[this] val catalog = spark.catalog

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    /** DATABASE DDLs */
    case DropDatabaseCommand(name, ignore, _) if !catalog.databaseExists(name) =>
      if (ignore) NoOpRunnableCommand
      else throw new NoSuchDatabaseException(name)

    /** TABLE DDLs */
    case drop @ DropTableCommand(id, ignore, _, _) =>
      try {
        drop.copy(tableName = qualifyTable(id))
      } catch {
        case _: AnalysisException if ignore => NoOpRunnableCommand
      }

    /** FUNCTION DDLs */
    case DropFunctionCommand(dbOpt, fn, ifExists, isTemp) =>
      withFunctionDDL(dbOpt, fn, isTemp) { (db, exists) =>
        if (!exists && ifExists) {
          NoOpRunnableCommand
        } else if (!exists) {
          throw new NoSuchFunctionException(db, fn)
        } else {
          DropFunctionCommand(Option(db), fn, ifExists, isTemp)
        }
      }

    /** Regular LogicalPlans */
    case _ => plan
  }

  /**
   * This function invokes the function DDL constructor with a fully qualified
   * [[FunctionIdentifier]] and an existence flag.
   */
  private[this] def withFunctionDDL(
      dbOpt: Option[String],
      fn: String,
      isTemp: Boolean)(
      f: (String, Boolean) => LogicalPlan): LogicalPlan = {
    val db = qualifyDatabaseInDDL(dbOpt, isTemp)
    val exists = try {
      // Function must exist and must be defined in the same scope (permanent or temporary).
      catalog.getFunction(db, fn).isTemporary == isTemp
    } catch {
      case _: NoSuchFunctionException => false
    }
    f(db, exists)
  }

  /**
   * Qualify the a database used for creating an object in the given database. This method throws
   * an [[AnalysisException]] if a user specifies a database for a temporary object. This method
   * uses the default database if no database has been defined for a permanent object.
   */
  private[this] def qualifyDatabaseInDDL(opt: Option[String], isTemp: Boolean): String = {
    opt match {
      case Some(db) if isTemp =>
        // NOTE: This invariant should be checked by the command itself.
        throw new IllegalArgumentException(
          "Specifying a database in a DDL statement for " +
            s"a temporary object is not allowed: '$db'")
      case Some(db) if !catalog.databaseExists(db) =>
        throw new NoSuchDatabaseException(db)
      case Some(db) => db
      case None if !isTemp => catalog.currentDatabase
      case None => null
    }
  }

  /**
   * Qualify a [[TableIdentifier]] or throw an [[AnalysisException]] if the table or the table's
   * database does not exist.
   */
  private[this] def qualifyTable(id: TableIdentifier): TableIdentifier = {
    val metadata = catalog.getTable(id.database.orNull, id.table)
    id.copy(database = Option(metadata.database))
  }
}

/**
 * A [[org.apache.spark.sql.catalyst.plans.logical.Command]] without any effect. This is used
 * as a replacement for commands that are effectively void.
 */
case object NoOpRunnableCommand extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = Seq.empty
}
