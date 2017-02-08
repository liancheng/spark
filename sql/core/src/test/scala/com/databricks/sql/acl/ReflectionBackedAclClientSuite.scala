/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import com.databricks.sql.DatabricksStaticSQLConf
import com.databricks.sql.acl.Action.{Modify, Select}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.test.SharedSQLContext

class ReflectionBackedAclClientSuite extends SparkFunSuite with SharedSQLContext {
  val tbl1 = Table(TableIdentifier("tbl_x", Some("db1")))
  val fn1 = Function(FunctionIdentifier("fn1", Some("db2")))
  val principal1 = NamedPrincipal("USER_1")
  val principal2 = NamedPrincipal("USER_2")

  lazy val client = new ReflectionBackedAclClient(spark)

  protected override def beforeAll(): Unit = {
    sparkConf.set(
      DatabricksStaticSQLConf.ACL_CLIENT_BACKEND.key,
      classOf[AclClientBackend].getCanonicalName)
    super.beforeAll()
  }

  private def sparkContextToken(token: String): Unit = {
    spark.sparkContext.setLocalProperty(TokenConf.TOKEN_KEY, token)
  }

  private def driverToken(token: String): Unit = {
    AclClientBackend.localContextToken = Option(token)
  }

  test("getValidPermissions") {
    sparkContextToken("usr1")
    driverToken(null)
    val input = Seq((tbl1, Select), (tbl1, Modify))
    assert(input.toSet === client.getValidPermissions(input))
    assert(AclClientBackend.token === "usr1")
    assert(AclClientBackend.lastCommandArguments === Seq(
      ("/CATALOG/`default`/DATABASE/`db1`/TABLE/`tbl_x`", "SELECT"),
      ("/CATALOG/`default`/DATABASE/`db1`/TABLE/`tbl_x`", "MODIFY")))
  }

  test("getOwners") {
    sparkContextToken("usr3")
    val input = Seq(tbl1, fn1)
    assert(client.getOwners(input) === Map((tbl1, principal1), (fn1, principal1)))
    assert(AclClientBackend.token === "usr3")
    assert(AclClientBackend.lastCommandArguments === Seq(
      "/CATALOG/`default`/DATABASE/`db1`/TABLE/`tbl_x`",
      "/CATALOG/`default`/DATABASE/`db2`/FUNCTION/`fn1`"))
  }

  test("listPermissions") {
    sparkContextToken("usr4")
    assert(client.listPermissions(None, tbl1) ===
      Seq(Permission(principal2, Select, tbl1)))
    assert(AclClientBackend.token === "usr4")
    assert(AclClientBackend.lastCommandArguments ===
      Seq("/CATALOG/`default`/DATABASE/`db1`/TABLE/`tbl_x`"))
    assert(client.listPermissions(Some(principal1), fn1) ===
      Seq(Permission(principal1, Select, fn1)))
    assert(AclClientBackend.lastCommandArguments === Seq(
      "USER_1",
      "/CATALOG/`default`/DATABASE/`db2`/FUNCTION/`fn1`"))
  }

  test("modify") {
    sparkContextToken("usr8")
    client.modify(Seq(
      GrantPermission(Permission(principal1, Select, tbl1)),
      RevokePermission(Permission(principal2, Select, fn1)),
      DropSecurable(fn1),
      Rename(fn1, fn1),
      CreateSecurable(tbl1),
      ChangeOwner(tbl1, principal2)))
    assert(AclClientBackend.token === "usr8")
    assert(AclClientBackend.lastCommandArguments === Seq(
      ("GRANT", "USER_1", "SELECT",
        "/CATALOG/`default`/DATABASE/`db1`/TABLE/`tbl_x`"),
      ("REVOKE", "USER_2", "SELECT",
        "/CATALOG/`default`/DATABASE/`db2`/FUNCTION/`fn1`"),
      ("DROP",
        "/CATALOG/`default`/DATABASE/`db2`/FUNCTION/`fn1`", "", ""),
      ("RENAME",
        "/CATALOG/`default`/DATABASE/`db2`/FUNCTION/`fn1`",
        "/CATALOG/`default`/DATABASE/`db2`/FUNCTION/`fn1`", ""),
      ("CREATE",
        "/CATALOG/`default`/DATABASE/`db1`/TABLE/`tbl_x`", "", ""),
      ("CHANGE_OWNER",
        "/CATALOG/`default`/DATABASE/`db1`/TABLE/`tbl_x`", "USER_2", "")))
  }

  test("Context token before driver") {
    sparkContextToken("sc")
    driverToken("driver")
    client.getValidPermissions(Seq.empty[(Securable, Action)])
    assert(AclClientBackend.token == "sc")

    driverToken(null)
    client.getValidPermissions(Seq.empty[(Securable, Action)])
    assert(AclClientBackend.token == "sc")
  }

  test("Driver token") {
    sparkContextToken(null)
    driverToken("driver")
    client.getValidPermissions(Seq.empty[(Securable, Action)])
    assert(AclClientBackend.token == "driver")
  }

  test("No token") {
    sparkContextToken(null)
    driverToken(null)
    intercept[SecurityException] {
      client.getValidPermissions(Seq.empty[(Securable, Action)])
    }
  }
}

object AclClientBackend {
  var token: String = _
  var lastCommandArguments: Seq[AnyRef] = Seq.empty
  var localContextToken: Option[String] = None
  def clear(): Unit = {
    token = null
    lastCommandArguments = Seq.empty
    localContextToken = None
  }
}

class AclClientBackend {
  def getTokenFromLocalContext: Option[String] = AclClientBackend.localContextToken

  def getValidPermissions(
      token: String,
      requests: Seq[(String, String)]): Set[(String, String)] = {
    AclClientBackend.token = token
    AclClientBackend.lastCommandArguments = requests.toSeq
    requests.toSet
  }

  def getOwners(
      token: String,
      securables: Seq[String]): Map[String, String] = {
    AclClientBackend.token = token
    AclClientBackend.lastCommandArguments = securables.toSeq
    securables.map(_ -> "USER_1").toMap
  }

  def listPermissions(
      token: String,
      principal: Option[String],
      securable: String): Seq[(String, String, String)] = {
    AclClientBackend.token = token
    AclClientBackend.lastCommandArguments = principal.toSeq ++ Seq(securable)
    Seq((principal.getOrElse("USER_2"), "SELECT", securable))
  }

  def modify(
      token: String,
      modifications: Seq[(String, String, String, String)]): Unit = {
    AclClientBackend.token = token
    AclClientBackend.lastCommandArguments = modifications
  }
}
