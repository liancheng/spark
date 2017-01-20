/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.test.TestHiveSingleton

/**
 * Test for the [[ResolveDropCommand]] rule.
 */
class ResolveDropCommandSuite extends PlanTest with TestHiveSingleton {
  def resolver: Rule[LogicalPlan] = new ResolveDropCommand(spark)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("create database db1")
    spark.sql("create database db2")
    spark.sql("create table db1.tbl1(id int)")
    spark.sql("create table db2.tbl2(id int)")
    spark.sql("create function db1.fn1 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs'")
    spark.sql("create function db2.fn2 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs'")
    spark.sql(
      "create temporary function fnt AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs'")
    spark.sql("create temporary view vw1 as select * from values 1,2,3 as t(id)")
    spark.sql("create view db2.vw2 as select * from values 1,2,3 as t(id)")
    spark.sql("use db2")
  }

  private def checkPlans[T](id1: T, id2: T, f: T => LogicalPlan): Unit = {
    comparePlans(resolver(f(id1)), f(id2))
  }

  private def checkNoOp(plan: LogicalPlan): Unit = {
    comparePlans(resolver(plan), NoOpRunnableCommand)
  }

  private def checkIdentity(plan: LogicalPlan): Unit = {
    comparePlans(resolver(plan), plan)
  }

  private def checkWithDatabase(f: String => LogicalPlan): Unit = {
    checkPlans("db2", "db2", f)
    intercept[NoSuchDatabaseException](resolver(f("db3")))
  }

  private def checkWithTable(f: TableIdentifier => LogicalPlan): Unit = {
    checkPlans(table("db2", "tbl2"), table("db2", "tbl2"), f)
    checkPlans(table("db1", "tbl1"), table("db1", "tbl1"), f)
    checkPlans(table(null, "tbl2"), table("db2", "tbl2"), f)
    val c = f.andThen(resolver.apply)
    intercept[NoSuchDatabaseException](c(table("db3", "tbl1")))
    intercept[NoSuchTableException](c(table("db1", "tbl2")))
    intercept[NoSuchTableException](c(table(null, "tbl1")))
  }

  private def table(db: String, tbl: String): TableIdentifier = {
    TableIdentifier(tbl, Option(db))
  }

  test("drop database") {
    checkWithDatabase(DropDatabaseCommand(_, ifExists = false, cascade = false))
    checkNoOp(DropDatabaseCommand("db3", ifExists = true, cascade = false))
  }

  test("drop table") {
    checkWithTable(DropTableCommand(_, ifExists = false, isView = false, purge = true))
    checkNoOp(
      DropTableCommand(table("db1", "tbl9"), ifExists = true, isView = false, purge = false))
  }

  test("drop function") {
    def dropFunc(db: String, fn: String, ifExists: Boolean, isTemp: Boolean): LogicalPlan = {
      DropFunctionCommand(Option(db), fn, ifExists, isTemp)
    }
    checkIdentity(dropFunc(null, "fnt", ifExists = false, isTemp = true))
    intercept[NoSuchFunctionException](resolver(
      dropFunc(null, "fnx", ifExists = false, isTemp = true)))
    checkNoOp(dropFunc(null, "fnx", ifExists = true, isTemp = true))

    checkIdentity(dropFunc("db2", "fn2", ifExists = false, isTemp = false))
    intercept[NoSuchFunctionException](resolver(
      dropFunc("db1", "fn2", ifExists = false, isTemp = false)))
    checkNoOp(dropFunc("db1", "fn2", ifExists = true, isTemp = false))

    checkPlans((null, "fn2", false, false), ("db2", "fn2", false, false), (dropFunc _).tupled)
    intercept[NoSuchFunctionException](resolver(
      dropFunc(null, "fn1", ifExists = false, isTemp = false)))
    checkNoOp(dropFunc(null, "fn1", ifExists = true, isTemp = false))
  }
}
