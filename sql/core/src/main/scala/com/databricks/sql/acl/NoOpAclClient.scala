/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.SparkSession

abstract class AbstractNoOpAclClient extends AclClient {
  type Request = (Securable, Action)

  override def getValidPermissions(
      requests: Seq[(Securable, Action)]): Set[Request] = requests.toSet
  override def getOwners(securables: Seq[Securable]): Map[Securable, Principal] = Map.empty
  override def modify(modifications: Seq[ModifyPermission]): Unit = {}
  override def listPermissions(
      principal: Option[Principal],
      securable: Securable): Seq[Permission] = {
    Seq.empty
  }
}

/**
 * A no-op [[AclClient]] implementation.
 */
object NoOpAclClient extends AbstractNoOpAclClient

class NoOpAclProvider extends AclProvider {
  override def create(session: SparkSession): AclClient = NoOpAclClient
}
