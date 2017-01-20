/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.catalog.{Catalog => PublicCatalog}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

class HiveCheckPermissions(catalog: PublicCatalog, aclClient: AclClient)
    extends CheckPermissions(catalog, aclClient) {

  override def commandToRequests(command: Command): Seq[Request] = command match {
    case ctas: CreateHiveTableAsSelectCommand =>
      queryToRequests(ctas.query) ++
        toSecurables(ctas.tableDesc.identifier.database).map(Action.Create.apply)

    case _ => super.commandToRequests(command)
  }

  override def exprNodeToRequests(expression: Expression): Seq[Request] = {
    def executeFunction(name: String) = {
      val parsedId = CatalystSqlParser.parseTableIdentifier(name)
      val securable = if (isTempDb(parsedId.database)) {
        AnonymousFunction
      } else {
        Function(FunctionIdentifier(parsedId.table, parsedId.database))
      }
      Seq(Request(securable, Action.Select))
    }

    expression match {
      case HiveSimpleUDF(name, _, _) => executeFunction(name)
      case HiveGenericUDF(name, _, _) => executeFunction(name)
      case HiveGenericUDTF(name, _, _) => executeFunction(name)
      case HiveUDAFFunction(name, _, _, _, _, _) => executeFunction(name)
      case _ => super.exprNodeToRequests(expression)
    }
  }
}
