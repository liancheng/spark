/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import scala.collection.mutable.ArrayBuffer

import com.databricks.sql.acl.Action._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSQLContext

class AclCommandSuite extends SparkFunSuite with SharedSQLContext {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("create table tbl_x(id int)")
  }

  val user1 = NamedPrincipal("user1")
  val tblX = Table(TableIdentifier("tbl_x"))
  val tblXq = Table(TableIdentifier("tbl_x", Some("default")))
  val tblY = Table(TableIdentifier("tbl_y"))

  def failOnNotExists(command: AclCommand): Unit = {
    val e = intercept[SecurityException](command.run(spark))
    assert(e.getMessage.contains("does not exist"))
  }

  def runWithClient(events: ModifyPermission*)(f: AclClient => AclCommand): Unit = {
    val client = new ModificationRecordingAclClient
    val command = f(client)
    command.run(spark)
    assert(client.recorder === events)
  }

  test("grant/revoke") {
    // Successful command
    val p1 = Permission(user1, Select, tblX)
    val p1q = Permission(user1, Select, tblXq)
    val p2 = Permission(user1, Modify, tblX)
    val p2q = Permission(user1, Modify, tblXq)
    val p3 = Permission(user1, Select, tblY)

    // Grant
    runWithClient(GrantPermission(p1q), GrantPermission(p2q)) { client =>
      GrantPermissionsCommand(client, Seq(p1, p2))
    }

    // Revoke
    runWithClient(RevokePermission(p1q), RevokePermission(p2q)) { client =>
      RevokePermissionsCommand(client, Seq(p1, p2))
    }

    // Fail command.
    failOnNotExists(GrantPermissionsCommand(NoOpAclClient, Seq(p3)))
    failOnNotExists(RevokePermissionsCommand(NoOpAclClient, Seq(p3)))
  }

  test("repair") {
    runWithClient(DropSecurable(tblY)) { client =>
      CleanPermissionsCommand(client, tblY)
    }

    runWithClient() { client =>
      CleanPermissionsCommand(client, tblX)
    }
  }

  test("change owner") {
    runWithClient(ChangeOwner(tblXq, user1)) { client =>
      ChangeOwnerCommand(client, tblX, user1)
    }
    failOnNotExists(ChangeOwnerCommand(NoOpAclClient, tblY, user1))
  }

  test("list permissions") {
    val p1 = Permission(user1, Select, tblXq)
    val p2 = Permission(user1, Modify, tblXq)
    val client = new AbstractNoOpAclClient {
      override def listPermissions(
          principal: Option[Principal],
          securable: Securable): Seq[Permission] = {
        Seq(p1, p2)
      }
    }

    val command = ShowPermissionsCommand(client, None, tblXq)
    val expectedResult =
      Row("user1", "SELECT", "TABLE", "`default`.`tbl_x`") ::
        Row("user1", "MODIFY", "TABLE", "`default`.`tbl_x`") :: Nil
    assert(command.run(spark) === expectedResult)
  }
}

class ModificationRecordingAclClient extends AbstractNoOpAclClient {
  val recorder: ArrayBuffer[ModifyPermission] = ArrayBuffer.empty
  def reset(): Unit = recorder.clear()
  override def modify(modifications: Seq[ModifyPermission]): Unit = {
    recorder ++= modifications
  }
}
