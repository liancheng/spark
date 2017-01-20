/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier

class PermissionCheckerSuite extends SparkFunSuite {

  /**
   * V--> T1
   * :--> T2
   */
  val innerViewSelectRequest = Request(Table(TableIdentifier("V")), Action.Select,
    Set(Request(Table(TableIdentifier("T1")), Action.Select),
      Request(Table(TableIdentifier("T2")), Action.Select)))

  /**
   * W--> T3
   * :-->  V--> T1
   *       :--> T2
   */
  val nestedViewSelectRequest = Request(Table(TableIdentifier("W")), Action.Select,
    Set(Request(Table(TableIdentifier("T3")), Action.Select), innerViewSelectRequest))

  test("Single securable checks") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U1"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val table = Table(TableIdentifier("T1"))

    Action.allIOActions.foreach { grantedAction =>
      val selectRequest = Request(table, grantedAction)
      inMemoryClient.clearAll
      inMemoryClient.setOwner(table, NamedPrincipal("U1"))
      inMemoryClient.grantPermission(Permission(NamedPrincipal("U2"), grantedAction, table))
      inMemoryClient.underlyingPrincipal = NamedPrincipal("U1")
      assert(permissionChecker(selectRequest))
      inMemoryClient.underlyingPrincipal = NamedPrincipal("U2")
      assert(permissionChecker(selectRequest))
      inMemoryClient.underlyingPrincipal = NamedPrincipal("U3")
      assert(!permissionChecker(selectRequest))

      Action.allIOActions.filter(a => a != grantedAction).foreach { action =>
        val request = Request(table, action)
        inMemoryClient.underlyingPrincipal = NamedPrincipal("U1")
        assert(permissionChecker(request))
        inMemoryClient.underlyingPrincipal = NamedPrincipal("U2")
        assert(!permissionChecker(request))
        inMemoryClient.underlyingPrincipal = NamedPrincipal("U3")
        assert(!permissionChecker(request))
      }
    }
  }

  test("Implied permissions") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U1"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val table = Table(TableIdentifier("T1"))
    Action.allIOActions.foreach { underlyingAction =>
      inMemoryClient.clearAll
      inMemoryClient.setOwner(table, NamedPrincipal("owner"))
      inMemoryClient.grantPermission(
        Permission(NamedPrincipal("U1"),
        Action.Grant(underlyingAction),
        table))
      inMemoryClient.underlyingPrincipal = NamedPrincipal("U1")
      assert(permissionChecker(Request(table, underlyingAction)))
      inMemoryClient.underlyingPrincipal = NamedPrincipal("owner")
      assert(permissionChecker(Request(table, underlyingAction)))
      Action.allIOActions.filter(_ != underlyingAction).foreach { otherAction =>
        inMemoryClient.underlyingPrincipal = NamedPrincipal("U1")
        assert(!permissionChecker(Request(table, otherAction)))
      }
    }
  }

  test("Only Select on views") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    var viewSelectRequest = innerViewSelectRequest.copy(action = Action.Modify)
    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("U"))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("U"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("U"))
    intercept[AssertionError] {
      permissionChecker(viewSelectRequest)
    }

    viewSelectRequest = Request(Table(TableIdentifier("W")), Action.Select,
      Set(Request(Table(TableIdentifier("T3")), Action.Select), viewSelectRequest))
    inMemoryClient.setOwner(Table(TableIdentifier("W")), NamedPrincipal("U"))
    inMemoryClient.setOwner(Table(TableIdentifier("T3")), NamedPrincipal("U"))
    intercept[AssertionError] {
      permissionChecker(viewSelectRequest)
    }
  }

  test("Same ownership of all tables in view") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val viewSelectRequest = innerViewSelectRequest

    /**
     * View V references T1 and T2.
     * If U has read permissions on V and V, T1 and T2 have the same owner
     * then the check should pass
     */
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("V"))))
    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O"))

    assert(permissionChecker(viewSelectRequest))
  }

  test("Different ownership of tables in view with no underlying read permissions") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val viewSelectRequest = innerViewSelectRequest

    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("V"))))
    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O2"))

    /**
     * If a table reference T2 in V has a different owner then U needs read permissions
     * on that table. It's not sufficient if the owner of V can read it.
     */
    assert(!permissionChecker(viewSelectRequest))
    inMemoryClient.grantPermission(Permission(NamedPrincipal("O"), Action.Select,
      Table(TableIdentifier("T2"))))
    assert(!permissionChecker(viewSelectRequest))
  }

  test("Different ownership of tables with underlying read permissions") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val viewSelectRequest = innerViewSelectRequest

    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("V"))))
    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O2"))

    /**
     * If a table reference has a different owner and U has read permissions on it
     * then U can read it, but not otherwise.
     */
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("T2"))))
    assert(permissionChecker(viewSelectRequest))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O3"))
    assert(!permissionChecker(viewSelectRequest))
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("T1"))))
    assert(permissionChecker(viewSelectRequest))
  }

  test("Read on underlying tables but not on view") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val viewSelectRequest = innerViewSelectRequest

    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O"))

    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("T1"))))
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("T2"))))
    assert(!permissionChecker(viewSelectRequest))
  }

  test("View chaining same owner") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)

    /**
     * If U has read permissions on W and W, V and T3 have the same owner
     * then the check should pass
     */
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("W"))))
    inMemoryClient.setOwner(Table(TableIdentifier("W")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T3")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O"))

    assert(permissionChecker(nestedViewSelectRequest))
  }

  test("Nested view with same owner and different constituent owners") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val viewSelectRequest = nestedViewSelectRequest
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("W"))))
    inMemoryClient.setOwner(Table(TableIdentifier("W")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T3")), NamedPrincipal("O"))

    /**
     * If all the owners are not the same, check that you require permissions
     */
    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O2"))
    assert(!permissionChecker(viewSelectRequest))

    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("V"))))
    assert(!permissionChecker(viewSelectRequest))
  }

  test("Nested view with different owner and same constituent owners") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val viewSelectRequest = nestedViewSelectRequest

    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("W"))))
    inMemoryClient.setOwner(Table(TableIdentifier("W")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T3")), NamedPrincipal("O"))

    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O2"))

    /**
     * If the owner of V owns all constituents and U has read permissions
     * on V, that is sufficient.
     */
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O2"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O2"))
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("V"))))

    assert(permissionChecker(viewSelectRequest))
  }

  test("Nested view breaks chain") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val viewSelectRequest = nestedViewSelectRequest

    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("W"))))
    inMemoryClient.setOwner(Table(TableIdentifier("W")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T3")), NamedPrincipal("O"))

    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O2"))

    /**
     * If the owner of an intermediate view is different from the parent view,
     * U needs permissions on V even if it has permissions on everything else
     * in the tree.
     */
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O"))
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("T1"))))
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("T2"))))
    assert(!permissionChecker(viewSelectRequest))

    /**
     * If permissions to V are granted, U has access
     */
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("V"))))
    assert(permissionChecker(viewSelectRequest))

    /**
     * If permissions to V are granted, but not on T1 and T2 which have the same
     * owner as the topmost W, the chain is still broken and the request fails.
     */
    inMemoryClient.revokePermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("T1"))))
    inMemoryClient.revokePermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("T2"))))
    assert(!permissionChecker(viewSelectRequest))
  }

  test("Nested view with different constituent owners") {
    val inMemoryClient = new InMemoryAclClient(NamedPrincipal("U"))
    val permissionChecker = new PermissionChecker(inMemoryClient)
    val viewSelectRequest = nestedViewSelectRequest

    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"),
      Action.Select, Table(TableIdentifier("W"))))
    inMemoryClient.setOwner(Table(TableIdentifier("W")), NamedPrincipal("O"))
    inMemoryClient.setOwner(Table(TableIdentifier("T3")), NamedPrincipal("O"))

    inMemoryClient.setOwner(Table(TableIdentifier("V")), NamedPrincipal("O2"))
    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O2"))
    inMemoryClient.setOwner(Table(TableIdentifier("T2")), NamedPrincipal("O3"))
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("V"))))

    /**
     * If the constituents of the nested view V have different owners the U needs explicit
     * read permissions on the constituents
     */
    assert(!permissionChecker(viewSelectRequest))
    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("T2"))))
    assert(permissionChecker(viewSelectRequest))

    inMemoryClient.setOwner(Table(TableIdentifier("T1")), NamedPrincipal("O4"))
    assert(!permissionChecker(viewSelectRequest))

    inMemoryClient.grantPermission(Permission(NamedPrincipal("U"), Action.Select,
      Table(TableIdentifier("T1"))))
    assert(permissionChecker(viewSelectRequest))
  }
}
