/*
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import scala.collection.mutable

class InMemoryAclClient(var underlyingPrincipal: Principal) extends AclClient {
  override def getValidPermissions(requests: Seq[(Securable, Action)])
    : Set[(Securable, Action)] = {
    requests.filter { pair =>
      val (securable, action) = pair
      allPermissions.contains(Permission(underlyingPrincipal, action, securable))
    }.toSet
  }

  override def getOwners(securables: Seq[Securable]): Map[Securable, Principal] = {
    val forSecurables = securables.toSet
    allPermissions.filter { perm =>
      perm.action == Action.Own && forSecurables.contains(perm.securable)
    }.map(perm => perm.securable -> perm.principal).toMap
  }

  override def listPermissions(
      principal: Option[Principal] = None,
      securable: Securable): Seq[Permission] = {
    allPermissions.filter { perm =>
      (principal.isEmpty || principal == Some(perm.principal)) && (securable == perm.securable)
    }.toSeq
  }

  override def modify(modifications: Seq[ModifyPermission]): Unit = {
    modifications.foreach {
      case GrantPermission(permission) => grantPermission(permission)
      case RevokePermission(permission) => revokePermission(permission)
      case Rename(before, after) => throw new NotImplementedError
      case DropSecurable(securable) => removeSecurable(securable)
      case CreateSecurable(securable) => setOwner(securable, underlyingPrincipal)
      case ChangeOwner(securable, next) => setOwner(securable, next)
    }
  }

  def grantPermission(perm: Permission): Unit = {
    allPermissions.add(perm)
  }

  def revokePermission(perm: Permission): Unit = {
    allPermissions.remove(perm)
  }

  def setOwner(securable: Securable, owner: Principal): Unit = {
    val oldOwners = allPermissions
      .filter(perm => perm.securable == securable && perm.action == Action.Own)
    assert(oldOwners.size <= 1)
    if (oldOwners.nonEmpty) {
      revokePermission(oldOwners.head)
    }
    grantPermission(Permission(owner, Action.Own, securable))
  }

  def removeSecurable(securable: Securable): Unit = {
    allPermissions.retain(_.securable != securable)
  }

  def removePrincipal(principal: Principal): Unit = {
    allPermissions.retain(_.principal != principal)
  }

  def clearAll(): Unit = {
    allPermissions.clear
  }

  private val allPermissions: mutable.HashSet[Permission] = mutable.HashSet.empty
}
