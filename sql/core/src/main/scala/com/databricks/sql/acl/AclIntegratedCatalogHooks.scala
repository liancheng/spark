/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import com.databricks.sql.acl.Action._

import org.apache.spark.sql.catalog.BaseCatalogHooks
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}

/**
 * These hooks accomplish two things:
 * 1. Keep the [[org.apache.spark.sql.catalyst.catalog.ExternalCatalog]] and the
 *    [[AclClient]] in sync. If a user makes a change in the catalog, the changes also get
 *    propagated to the ACL store. It is assumed the current principal holds sufficient
 *    permissions to execute to make the given modification.
 * 2. Secure the metadata retrieval path. All List*, Get* & Exists* function are checked for read
 *    metadata permissions.
 */
class AclIntegratedCatalogHooks(client: AclClient) extends BaseCatalogHooks {
  override def beforeListDatabases(pattern: Option[String]): Unit = {
    readMetadata(Catalog)
  }

  override def beforeCreateDatabase(db: String): Unit = {
    create(Database(db))
  }

  override def afterDropDatabase(db: String): Unit = {
    drop(Database(db))
  }

  override def beforeRenameDatabase(oldName: String, newName: String): Unit = {
    rename(Database(oldName), Database(newName))
  }

  override def beforeListTables(db: String, pattern: Option[String]): Unit = {
    readMetadata(Catalog, Database(db))
  }

  override def beforeCreateTable(db: String, table: String): Unit = {
    create(Table(db, table))
  }

  override def afterDropTable(db: String, table: String): Unit = {
    drop(Table(db, table))
  }

  override def beforeRenameTable(db: String, oldName: String, newName: String): Unit = {
    rename(Table(db, oldName), Table(db, newName))
  }

  override def beforeListFunctions(db: String, pattern: Option[String]): Unit = {
    readMetadata(Catalog, Database(db))
  }

  override def beforeCreateFunction(db: String, fn: String): Unit = {
    create(Function(db, fn))
  }

  override def afterDropFunction(db: String, fn: String): Unit = {
    drop(Function(db, fn))
  }

  override def beforeRenameFunction(db: String, oldName: String, newName: String): Unit = {
    rename(Function(db, oldName), Function(db, newName))
  }

  private def readMetadata(securables: Securable*): Unit = {
    if (client.getValidPermissions(securables.map(s => s -> ReadMetadata)).isEmpty) {
      throw new SecurityException(s"permission denied to read metadata for ${securables.last}")
    }
  }

  private def create(securable: Securable): Unit = {
    client.modify(CreateSecurable(securable) :: Nil)
  }

  private def rename(oldName: Securable, newName: Securable): Unit = {
    client.modify(Rename(oldName, newName) :: Nil)
  }

  private def drop(securable: Securable): Unit = {
    client.modify(DropSecurable(securable) :: Nil)
  }
}
