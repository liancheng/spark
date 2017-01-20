/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.mockito.Mockito._

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, OneRowRelation}
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveGenericUDTF, HiveSimpleUDF, HiveUDAFFunction}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.types.StructType

class HiveCheckPermissionRuleSuite extends CheckRequests {

  override val checkRule: CheckPermissions = new HiveCheckPermissions(catalog, null)

  ignore("Named UDFs") {
    /**
     * Permanent functions require permissions, temporary functions require anonymous
     * permissions.
     */
    val query = Filter(HiveSimpleUDF("db.udf_simple", null,
        Seq(HiveGenericUDF("db.udf_generic", null, Nil))),
      Filter(HiveGenericUDTF("db.udtf", null, Nil),
        Filter(HiveUDAFFunction("udaff", null, Nil), OneRowRelation)))
    val expected = Set(
      Request(Function(FunctionIdentifier("udf_simple", Some("db"))), Action.Select),
      Request(Function(FunctionIdentifier("udf_generic", Some("db"))), Action.Select),
      Request(Function(FunctionIdentifier("udtf", Some("db"))), Action.Select),
      Request(AnonymousFunction, Action.Select)
    )
    checkAnswer(query, expected)
  }

  test("CTAS") {
    when(catalog.currentDatabase).thenReturn("PERM1")

    val query = CreateHiveTableAsSelectCommand(CatalogTable(TableIdentifier("T1"),
      CatalogTableType.MANAGED, CatalogStorageFormat.empty, new StructType),
      OneRowRelation, false)
    val expected = Set(Request(Database("PERM1"), Action.Create))
    checkAnswer(query, expected)
  }
}
