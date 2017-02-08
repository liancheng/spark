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

import org.apache.spark.sql.SparkSession

/**
 * This [[AclClient]] uses reflection to connect to a similar ACL Client on the databricks side.
 */
class ReflectionBackedAclClient(session: SparkSession) extends AclClient {
  // scalastyle:off classforname
  private val backendClazz =
    Class.forName(session.conf.get(DatabricksStaticSQLConf.ACL_CLIENT_BACKEND.key))
  // scalastyle:on classforname

  private[this] lazy val backend = backendClazz.newInstance()

  private[this] lazy val getTokenFromLocalContextMethod = {
    backendClazz.getMethod("getTokenFromLocalContext")
  }

  private[this] lazy val getValidPermissionsMethod = {
    backendClazz.getMethod(
      "getValidPermissions",
      classOf[String],
      classOf[Seq[(String, String)]])
  }

  private[this] lazy val getOwnersMethod = {
    backendClazz.getMethod(
      "getOwners",
      classOf[String],
      classOf[Seq[String]])
  }

  private[this] lazy val listPermissionsMethod = {
    backendClazz.getMethod(
      "listPermissions",
      classOf[String],
      classOf[Option[String]],
      classOf[String])
  }

  private[this] lazy val modifyMethod = {
    backendClazz.getMethod(
      "modify",
      classOf[String],
      classOf[Seq[(String, String, String, String)]])
  }

  override def getValidPermissions(requests: Seq[(Securable, Action)]): Set[(Securable, Action)] = {
    val argument = requests.map { case (securable, action) =>
      (securable.name, action.name)
    }
   val result = getValidPermissionsMethod
     .invoke(backend, token, argument)
     .asInstanceOf[Set[(String, String)]]
   result.map { case (s: String, a: String) =>
     (Securable.fromString(s), Action.fromString(a))
    }
  }

  override def getOwners(securables: Seq[Securable]): Map[Securable, Principal] = {
    val result = getOwnersMethod
      .invoke(backend, token, securables.map(_.name))
      .asInstanceOf[Map[String, String]]
    result.map { case (s: String, p: String) =>
      (Securable.fromString(s), NamedPrincipal(p))
    }
  }

  override def listPermissions(
      principal: Option[Principal],
      securable: Securable): Seq[Permission] = {
    val result = listPermissionsMethod
      .invoke(backend, token, principal.map(_.name), securable.name)
      .asInstanceOf[Seq[(String, String, String)]]
    result.map { case (p: String, a: String, s: String) =>
      Permission(NamedPrincipal(p), Action.fromString(a), Securable.fromString(s))
    }
  }

  override def modify(modifications: Seq[ModifyPermission]): Unit = {
    val argument = modifications.map {
      case GrantPermission(Permission(principal, action, securable)) =>
        ("GRANT", principal.name, action.name, securable.name)
      case RevokePermission(Permission(principal, action, securable)) =>
        ("REVOKE", principal.name, action.name, securable.name)
      case DropSecurable(securable) =>
        ("DROP", securable.name, "", "")
      case Rename(from, to) =>
        ("RENAME", from.name, to.name, "")
      case CreateSecurable(securable) =>
        ("CREATE", securable.name, "", "")
      case ChangeOwner(securable, next) =>
        ("CHANGE_OWNER", securable.name, next.name, "")
    }
    modifyMethod.invoke(backend, token, argument)
  }

  private def token: String = {
    Option(session.sparkContext.getLocalProperty(TokenConf.TOKEN_KEY))
      .orElse(getTokenFromLocalContextMethod.invoke(backend).asInstanceOf[Option[String]])
      .getOrElse(throw new SecurityException("No token to authorize principal"))
  }
}

object TokenConf {
  val TOKEN_KEY = "spark.databricks.token"
}

class ReflectionBackedAclProvider extends AclProvider {
  override def create(session: SparkSession): AclClient = {
    new ReflectionBackedAclClient(session)
  }
}
