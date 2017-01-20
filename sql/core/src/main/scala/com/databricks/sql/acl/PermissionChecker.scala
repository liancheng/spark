/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

class PermissionChecker(aclClient: AclClient) extends (Request => Boolean) {
  /**
   * Check if an underlying principal has permissions to execute the request.
   * For nested requests implying permissions on views, only Select permissions
   * are supported (an assertion is thrown otherwise).
   * Requests on securables with no nesting (i.e. no children, e.g. tables)
   * are checked by a single lookup to the aclStore.
   * Select requests on views follow the principle of ownership chaining
   * If an owner of a view O gives permission to U, she implicitly gives permission
   * on constituents of O that she owns, so we should skip the permission check on those
   * constituents.
   * Select requests on views are resolved as follows:
   * First we check if the principal (P) can select from the view V. If not, fail.
   * Then for each of the view constituents T, we compare the owner of V (=O)
   * to the owner of T
   * If T is NOT owned by O, we check if P can Select on T and recurse on the children of T.
   * If T is owned by O, we do not need to check permissions on T itself
   *  but we will recursively check on the constituents of T
   * @return True if the underlying principal has permissions for the request
   */
  def apply(request: Request): Boolean = {
    val permissionsToCheck = flattenRequestTree(request)
    val validPermissions = aclClient.getValidPermissions(permissionsToCheck.toSeq)
    val securableOwners = aclClient.getOwners(permissionsToCheck.map(_._1).toSeq)

    def chainCheck(requests: Set[Request], parentOwner: Principal): Boolean = {
      requests.forall { request =>
        assert(request.children.isEmpty ||
          request.action == Action.Select ||
          request.action == Action.ReadMetadata)
        val owner = securableOwners.get(request.securable)
        assert(owner != Some(UnknownPrincipal))
        val hasPermission = getSufficient(request.securable, request.action)
          .exists(validPermissions.contains)
        (owner == Some(parentOwner) || hasPermission) &&
          chainCheck(request.children, owner.getOrElse(UnknownPrincipal))
      }
    }

    chainCheck(Set(request), UnknownPrincipal)
  }

  private def flattenRequestTree(request: Request)
    : Set[(Securable, Action)] = {
    request.children.flatMap(childReq => flattenRequestTree(childReq)) ++
      getSufficient(request.securable, request.action)
  }

  private def getSufficient(securable: Securable, action: Action): Set[(Securable, Action)] = {
    Set(
      (securable, action),
      (securable, Action.Grant(action)),
      (securable, Action.Own)
    ).filter(_._2.validateRequest(securable))
  }
}
