/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalog

import org.apache.spark.annotation.DeveloperApi

/**
 * The [[CatalogHooks]] trait provides a way to hook into most of the communication with the
 * [[org.apache.spark.sql.catalyst.catalog.ExternalCatalog]]. This currently provides before and
 * after hooks for List, Create, Drop, Alter, Exists, Get and Rename calls for Database, Table and
 * Function objects.
 *
 * This API is guaranteed to stay stable over multiple Spark versions.
 */
@DeveloperApi
trait CatalogHooks {
  // --------------------------------------------
  // Databases.
  // --------------------------------------------
  def beforeListDatabases(pattern: Option[String]): Unit
  def afterListDatabases(pattern: Option[String]): Unit
  def beforeCreateDatabase(db: String): Unit
  def afterCreateDatabase(db: String): Unit
  def beforeDropDatabase(db: String): Unit
  def afterDropDatabase(db: String): Unit
  def beforeAlterDatabase(db: String): Unit
  def afterAlterDatabase(db: String): Unit
  def beforeExistsDatabase(db: String): Unit
  def afterExistsDatabase(db: String): Unit
  def beforeGetDatabase(db: String): Unit
  def afterGetDatabase(db: String): Unit
  def beforeRenameDatabase(oldName: String, newName: String): Unit
  def afterRenameDatabase(oldName: String, newName: String): Unit

  // --------------------------------------------
  // Tables.
  // --------------------------------------------
  def beforeListTables(db: String, pattern: Option[String]): Unit
  def afterListTables(db: String, pattern: Option[String]): Unit
  def beforeCreateTable(db: String, table: String): Unit
  def afterCreateTable(db: String, table: String): Unit
  def beforeDropTable(db: String, table: String): Unit
  def afterDropTable(db: String, table: String): Unit
  def beforeAlterTable(db: String, table: String): Unit
  def afterAlterTable(db: String, table: String): Unit
  def beforeExistsTable(db: String, table: String): Unit
  def afterExistsTable(db: String, table: String): Unit
  def beforeGetTable(db: String, table: String): Unit
  def afterGetTable(db: String, table: String): Unit
  def beforeRenameTable(db: String, oldName: String, newName: String): Unit
  def afterRenameTable(db: String, oldName: String, newName: String): Unit

  // --------------------------------------------
  // Functions.
  // --------------------------------------------
  def beforeListFunctions(db: String, pattern: Option[String]): Unit
  def afterListFunctions(db: String, pattern: Option[String]): Unit
  def beforeCreateFunction(db: String, function: String): Unit
  def afterCreateFunction(db: String, function: String): Unit
  def beforeDropFunction(db: String, function: String): Unit
  def afterDropFunction(db: String, function: String): Unit
  def beforeAlterFunction(db: String, function: String): Unit
  def afterAlterFunction(db: String, function: String): Unit
  def beforeExistsFunction(db: String, function: String): Unit
  def afterExistsFunction(db: String, function: String): Unit
  def beforeGetFunction(db: String, function: String): Unit
  def afterGetFunction(db: String, function: String): Unit
  def beforeRenameFunction(db: String, oldName: String, newName: String): Unit
  def afterRenameFunction(db: String, oldName: String, newName: String): Unit
}

/**
 * Catalog hooks that do nothing.
 */
abstract class BaseCatalogHooks extends CatalogHooks {
  override def beforeListDatabases(pattern: Option[String]): Unit = { }
  override def afterListDatabases(pattern: Option[String]): Unit = { }
  override def beforeCreateDatabase(db: String): Unit = { }
  override def afterCreateDatabase(db: String): Unit = { }
  override def beforeDropDatabase(db: String): Unit = { }
  override def afterDropDatabase(db: String): Unit = { }
  override def beforeAlterDatabase(db: String): Unit = { }
  override def afterAlterDatabase(db: String): Unit = { }
  override def beforeExistsDatabase(db: String): Unit = { }
  override def afterExistsDatabase(db: String): Unit = { }
  override def beforeGetDatabase(db: String): Unit = { }
  override def afterGetDatabase(db: String): Unit = { }
  override def beforeRenameDatabase(oldName: String, newName: String): Unit = { }
  override def afterRenameDatabase(oldName: String, newName: String): Unit = { }
  override def beforeListTables(db: String, pattern: Option[String]): Unit = { }
  override def afterListTables(db: String, pattern: Option[String]): Unit = { }
  override def beforeCreateTable(db: String, table: String): Unit = { }
  override def afterCreateTable(db: String, table: String): Unit = { }
  override def beforeDropTable(db: String, table: String): Unit = { }
  override def afterDropTable(db: String, table: String): Unit = { }
  override def beforeAlterTable(db: String, table: String): Unit = { }
  override def afterAlterTable(db: String, table: String): Unit = { }
  override def beforeExistsTable(db: String, table: String): Unit = { }
  override def afterExistsTable(db: String, table: String): Unit = { }
  override def beforeGetTable(db: String, table: String): Unit = { }
  override def afterGetTable(db: String, table: String): Unit = { }
  override def beforeRenameTable(db: String, oldName: String, newName: String): Unit = { }
  override def afterRenameTable(db: String, oldName: String, newName: String): Unit = { }
  override def beforeListFunctions(db: String, pattern: Option[String]): Unit = { }
  override def afterListFunctions(db: String, pattern: Option[String]): Unit = { }
  override def beforeCreateFunction(db: String, function: String): Unit = { }
  override def afterCreateFunction(db: String, function: String): Unit = { }
  override def beforeDropFunction(db: String, function: String): Unit = { }
  override def afterDropFunction(db: String, function: String): Unit = { }
  override def beforeAlterFunction(db: String, function: String): Unit = { }
  override def afterAlterFunction(db: String, function: String): Unit = { }
  override def beforeExistsFunction(db: String, function: String): Unit = { }
  override def afterExistsFunction(db: String, function: String): Unit = { }
  override def beforeGetFunction(db: String, function: String): Unit = { }
  override def afterGetFunction(db: String, function: String): Unit = { }
  override def beforeRenameFunction(db: String, oldName: String, newName: String): Unit = { }
  override def afterRenameFunction(db: String, oldName: String, newName: String): Unit = { }
}
