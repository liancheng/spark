/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

trait AclClient {
  /**
   * Given a set of actions on securables, returns the subset which the underlying principal
   * has permissions to execute.
   *
   * @param requests to check.
   */
  def getValidPermissions(requests: Seq[(Securable, Action)]): Set[(Securable, Action)]

  /**
   * Retrieve the owners for the given securables.
   */
  def getOwners(securables: Seq[Securable]): Map[Securable, Principal]

  /**
   * List the permission for an (optional) principal and an (optional) object securable. The
   * principal issuing this command must meet one of the following requirements:
   * 1. The principal must be equal to or an (indirect) member of the given principal.
   * 2. The principal must own the given securable or one of its hierarchical parents.
   * A [[SecurityException]] is thrown when neither of these requirements is met.
   */
  @throws[SecurityException]("permission denied")
  def listPermissions(
      principal: Option[Principal] = None,
      securable: Securable): Seq[Permission]

  /**
   * Modify the current permissions. The current principal must hold the correct permissions to
   * execute all of the given modifications. A [[SecurityException]] will be thrown if this
   * condition is not met, and none of the modifications will be executed.
   */
  @throws[SecurityException]("permission denied")
  def modify(modifications: Seq[ModifyPermission]): Unit
}

/**
 * Modify permission command.
 */
sealed trait ModifyPermission

/**
 * Grant a permission to a user.
 *
 * This user issuing the command must hold a GRANT permission for the given ActionType on the
 * given object.
 */
case class GrantPermission(permission: Permission) extends ModifyPermission {
  require(permission.validateGrant, s"$permission cannot be granted.")
}

/**
 * Revoke a permission from a user.
 */
case class RevokePermission(permission: Permission) extends ModifyPermission {
  require(permission.validateGrant, s"$permission cannot be revoked.")}

/**
 * Remove all permissions for the given [[Securable]].
 */
case class DropSecurable(securable: Securable) extends ModifyPermission

/**
 * Rename a [[Securable]].
 */
case class Rename(before: Securable, after: Securable) extends ModifyPermission

/**
 * Create a new securable which will be owned by the current user.
 */
case class CreateSecurable(securable: Securable) extends ModifyPermission

/**
 * Change the owner of the [[Securable]]
 */
case class ChangeOwner(securable: Securable, next: Principal) extends ModifyPermission
