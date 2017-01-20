/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import scala.collection.mutable

import org.slf4j.LoggerFactory

/**
 * An in-memory (ephermeral) backend for testing SQL access control in DB Spark. This backend
 * implements the same interface as the actual backend in Universe.
 *
 * The backend does not implement user management, so permissions can be defined for any user.
 * There is a hardcoded super user, named `super`, that be used to set up tests.
 *
 * The state of the backend can be cleared by executing the following SQL command:
 * {{{
 *   MSCK REPAIR DATABASE __ALL__ PRIVILEGES;
 * }}}
 */
class SharedAclBackend {
  import SharedAclBackend._

  def getValidPermissions(
      token: String,
      requests: Seq[(String, String)]): Set[(String, String)] = {
    val validPermissions = if (isSuperUser(token)) {
      requests.toSet
    } else {
      val permissionsForToken = permissions.synchronized {
        permissions.filter(_._1 == token).map(x => x._3 -> x._2)
      }
      // TODO should we include the hierarchical permissions?
      requests.toSet.intersect(permissionsForToken)
    }
    log.info(s"getValidPermissions[token=$token, " +
      s"requests=${requests.mkString("[", ",", "]")}, " +
      s"validRequests=${validPermissions.mkString("[", ",", "]")}]")
    validPermissions
  }

  /**
   * Get the owners of the give securables.
   */
  def getOwners(
      token: String,
      securables: Seq[String]): Map[String, String] = {
    // TODO this is a bit funny to me. Should we always be able to resolve the owner? Or should we
    // at least hold some permission for this?
    val owners = permissions.synchronized {
      permissions.collect {
        case (owner, "OWN", securable) if securables.exists(_ == securable) => securable -> owner
      }
    }
    log.info(s"getOwners[token=$token, " +
      s"securables=${securables.mkString("[", ",", "]")}, " +
      s"owners=${owners.mkString("[", ",", "]")}]")
    owners.toMap
  }

  /**
   * List the permissions defined for the given securable and principal (optional). This function
   * will only return permissions iff:
   * - the current user is the super user.
   * - the current user is the user for which we are listing permissions.
   * - the current user is the owner of the securable.
   */
  def listPermissions(
      token: String,
      principal: Option[String],
      securable: String): Seq[(String, String, String)] = {
    val result = permissions.synchronized {
      // TODO should we check the hierarchical parent?
      def isOwner = permissions.exists(x => x._1 == token && x._2 == "OWN" && x._3 == securable)
      if (isSuperUser(token) || principal.exists(_ == token) || isOwner) {
        permissions.filter(x => principal.forall(_ == x._1) && x._3 == securable).toSeq
      } else {
        Seq.empty
      }
    }
    log.info(s"listPermissions[token=$token, " +
      s"principal=$principal, " +
      s"securable=$securable, " +
      s"permissions=${result.mkString("[", ",", "]")}]")
    result
  }

  /**
   * Modify the permissions. There are different modifications:
   * - GRANT: Grants a prinicipal the permission to execute an action on a securable.
   * - REVOKE: Revokes a prinicipal's permission to execute an action on a securable.
   * - DROP: Drops all permissions for a securable. If you drop permissions on a database named
   *         `__ALL__` then we drop all permissions (this is for testing only).
   * - RENAME: Rename a securable, and update the permissions accordingly.
   * - CREATE: Create a new Securable. This creates an OWN permission for the current token.
   * - CHANGE_OWNER: Change the owner of a securable. Update the current permission.
   *
   * @param token of the current user.
   * @param modifications to apply.
   */
  def modify(
      token: String,
      modifications: Seq[(String, String, String, String)]): Unit = {
    permissions.synchronized {
      modifications.foreach {
        case ("GRANT", principal, "OWN", securable) =>
          throw new SecurityException("Cannot grant OWN permission.")
        case ("GRANT", principal, action, securable) =>
          permissions += ((principal, action, securable))
        case ("REVOKE", principal, "OWN", securable) =>
          throw new SecurityException("Cannot revoke OWN permissions.")
        case ("REVOKE", principal, action, securable) =>
          permissions -= ((principal, action, securable))
        case ("DROP", "/CATALOG/`default`/DATABASE/`__ALL__`", "", "") =>
          permissions.clear()
        case ("DROP", securable, "", "") =>
          permissions.retain(_._3 != securable)
        case ("RENAME", from, to, "") =>
          val toBeRenamed = permissions.filter(_._3 == from)
          permissions --= toBeRenamed
          permissions ++= toBeRenamed.map(x => x.copy(_3 = to))
        case ("CREATE", securable, "", "") =>
          if (permissions.exists(_._3 == securable)) {
            throw new SecurityException(
              s"Permissions for securable $securable already exist")
          }
          permissions += ((token, "OWN", securable))
        case ("CHANGE_OWNER", securable, next, "") =>
          permissions.retain(p => p._2 != "OWN" || p._3 != securable)
          permissions += ((next, "OWN", securable))
      }
    }
    log.info(s"modify[token=$token, modifications=${modifications.mkString("[", ",", "]")}]")
  }
}

object SharedAclBackend {
  private val log = LoggerFactory.getLogger(classOf[SharedAclBackend].getCanonicalName)

  // Check if the current user is the super user.
  private def isSuperUser(token: String): Boolean = token == "super"

  // Permissions: Principal -> Action -> Secureable
  private val permissions = mutable.Set.empty[(String, String, String)]
}
