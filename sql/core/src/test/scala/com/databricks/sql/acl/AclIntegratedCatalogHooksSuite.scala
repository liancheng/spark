/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import scala.collection.mutable

import com.databricks.sql.acl.Action.ReadMetadata

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalog.CatalogHooks

/**
 * An extremely mechanical test to check if the [[AclIntegratedCatalogHooks]] invoke the correct
 * methods on the [[AclClient]].
 */
class AclIntegratedCatalogHooksSuite extends SparkFunSuite {
  val db1 = Database("db1")
  val db2 = Database("db2")
  val tbl1 = Table("db1", "tbl1")
  val tbl2 = Table("db2", "tbl2")
  val fn1 = Function("db1", "fn1")
  val fn2 = Function("db2", "fn2")
  val catAction = Catalog -> ReadMetadata
  val db1Action = db1 -> ReadMetadata
  val db2Action = db2 -> ReadMetadata
  val tbl1Action = tbl1 -> ReadMetadata
  val tbl2Action = tbl2 -> ReadMetadata
  val fn1Action = fn1 -> ReadMetadata
  val fn2Action = fn2 -> ReadMetadata
  val permissions: Set[(Securable, Action)] = Set(db1Action, tbl1Action, fn1Action)

  def createRecordingAclClient(recorder: mutable.Buffer[AnyRef]): AclClient = {
    val handler = new InvocationHandler {
      override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
        val nameArgs = method.getName +: args.toSeq
        recorder ++= nameArgs
        nameArgs match {
          case Seq("getValidPermissions", requests: Traversable[(Securable, Action)]) =>
            permissions.intersect(requests.toSet)
          case _ =>
            null
        }
      }
    }
    // Create the proxy.
    val proxy = Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array(classOf[AclClient]),
      handler)
    proxy.asInstanceOf[AclClient]
  }

  def assertInvoke(arguments: AnyRef*)(f: CatalogHooks => Unit): Unit = {
    val recorder = mutable.Buffer.empty[AnyRef]
    val client = createRecordingAclClient(recorder)
    f(new AclIntegratedCatalogHooks(client))
    assert(arguments === recorder)
  }

  def interceptInvoke(arguments: AnyRef*)(f: CatalogHooks => Unit): Unit = {
    val recorder = mutable.Buffer.empty[AnyRef]
    val client = createRecordingAclClient(recorder)
    intercept[SecurityException] {
      f(new AclIntegratedCatalogHooks(client))
    }
    assert(arguments === recorder)
  }

  test("listing") {
    interceptInvoke("getValidPermissions", catAction :: Nil) {
      _.beforeListDatabases(None)
    }
    assertInvoke() {
      _.afterListDatabases(None)
    }
    assertInvoke("getValidPermissions", catAction :: db1Action :: Nil) {
      _.beforeListTables("db1", None)
    }
    interceptInvoke("getValidPermissions", catAction :: db2Action :: Nil) {
      _.beforeListTables("db2", None)
    }
    assertInvoke() {
      _.afterListTables("db1", None)
    }
    assertInvoke("getValidPermissions", catAction :: db1Action :: Nil) {
      _.beforeListFunctions("db1", None)
    }
    interceptInvoke("getValidPermissions", catAction :: db2Action :: Nil) {
      _.beforeListFunctions("db2", None)
    }
    assertInvoke() {
      _.afterListFunctions("db1", None)
    }
  }

  test("create") {
    assertInvoke("modify", CreateSecurable(db1) :: Nil) {
      _.beforeCreateDatabase("db1")
    }
    assertInvoke() {
      _.afterCreateDatabase("db1")
    }
    assertInvoke("modify", CreateSecurable(tbl1) :: Nil) {
      _.beforeCreateTable("db1", "tbl1")
    }
    assertInvoke() {
      _.afterCreateTable("db1", "tbl1")
    }
    assertInvoke("modify", CreateSecurable(fn1) :: Nil) {
      _.beforeCreateFunction("db1", "fn1")
    }
    assertInvoke() {
      _.afterCreateFunction("db1", "fn1")
    }
  }

  test("alter") {
    assertInvoke() {
      _.beforeAlterDatabase("db1")
    }
    assertInvoke() {
      _.afterAlterDatabase("db1")
    }
    assertInvoke() {
      _.beforeAlterTable("db1", "tbl1")
    }
    assertInvoke() {
      _.afterAlterTable("db1", "tbl1")
    }
    assertInvoke() {
      _.beforeAlterFunction("db1", "fn1")
    }
    assertInvoke() {
      _.afterAlterFunction("db1", "fn1")
    }
  }

  test("rename") {
    assertInvoke("modify", Rename(db1, Database("db2")) :: Nil) {
      _.beforeRenameDatabase("db1", "db2")
    }
    assertInvoke() {
      _.afterRenameDatabase("db1", "db2")
    }
    assertInvoke("modify", Rename(tbl1, Table("db1", "tbl2")) :: Nil) {
      _.beforeRenameTable("db1", "tbl1", "tbl2")
    }
    assertInvoke() {
      _.afterRenameTable("db1", "tbl1", "tbl2")
    }
    assertInvoke("modify", Rename(fn1, Function("db1", "fn2")) :: Nil) {
      _.beforeRenameFunction("db1", "fn1", "fn2")
    }
    assertInvoke() {
      _.afterRenameFunction("db1", "fn1", "fn2")
    }
  }

  test("drop") {
    assertInvoke() {
      _.beforeDropDatabase("db1")
    }
    assertInvoke("modify", DropSecurable(db1) :: Nil) {
      _.afterDropDatabase("db1")
    }
    assertInvoke() {
      _.beforeDropTable("db1", "tbl1")
    }
    assertInvoke("modify", DropSecurable(tbl1) :: Nil) {
      _.afterDropTable("db1", "tbl1")
    }
    assertInvoke() {
      _.beforeDropFunction("db1", "fn1")
    }
    assertInvoke("modify", DropSecurable(fn1) :: Nil) {
      _.afterDropFunction("db1", "fn1")
    }
  }
}
