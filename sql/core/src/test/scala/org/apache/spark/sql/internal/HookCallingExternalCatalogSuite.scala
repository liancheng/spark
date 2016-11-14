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
package org.apache.spark.sql.internal

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.file.{Files, Path}

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalog.CatalogHooks
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{DatabaseAlreadyExistsException, FunctionAlreadyExistsException, NoSuchDatabaseException, NoSuchFunctionException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.StructType

/**
 * Tests for the [[HookCallingExternalCatalog]]
 */
class HookCallingExternalCatalogSuite extends SparkFunSuite {
  type Event = Seq[AnyRef]
  private def testWithCatalog(
      name: String)(
      f: (ExternalCatalog, Seq[Event] => Unit) => Unit): Unit = {

    val recorder = mutable.Buffer.empty[Event]
    // We create a hooks object by creating a proxy for the [[CatalogHooks]] interface. This
    // allows us to easily record the name of the hook invoked and the arguments passed. The
    // recorded events are persisted in the recorder buffer.
    val hooks = {
      val handler = new InvocationHandler {
        override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
          recorder += method.getName +: args.toSeq
          null
        }
      }
      // Create the proxy.
      val proxy = Proxy.newProxyInstance(
        getClass.getClassLoader,
        Array(classOf[CatalogHooks]),
        handler)
      proxy.asInstanceOf[CatalogHooks]
    }
    val catalog = new HookCallingExternalCatalog(new InMemoryCatalog, hooks)
    test(name) {
      f(catalog, checkEvents(recorder))
    }
  }

  private def checkEvents(actual: mutable.Buffer[Event])(expected: Seq[Event]): Unit = {
    assert(actual === expected)
    actual.clear()
  }

  private def createDbDefinition(uri: String): CatalogDatabase = {
    CatalogDatabase(name = "db5", description = "", locationUri = uri, Map.empty)
  }

  private def createDbDefinition(): CatalogDatabase = {
    createDbDefinition(preparePath(Files.createTempDirectory("db_")))
  }

  private def preparePath(path: Path): String = {
    path.normalize().toUri.toString.stripSuffix("/")
  }

  testWithCatalog("database") { (catalog, checkEvents) =>
    // CREATE
    val dbDefinition = createDbDefinition()

    catalog.createDatabase(dbDefinition, ignoreIfExists = false)
    checkEvents(
      Seq("beforeCreateDatabase", "db5") ::
        Seq("afterCreateDatabase", "db5") :: Nil)

    catalog.createDatabase(dbDefinition, ignoreIfExists = true)
    checkEvents(Nil)

    intercept[DatabaseAlreadyExistsException] {
      catalog.createDatabase(dbDefinition, ignoreIfExists = false)
    }
    checkEvents(Seq("beforeCreateDatabase", "db5") :: Nil)

    // LIST
    assert(catalog.listDatabases() === Seq("db5"))
    checkEvents(
      Seq("beforeListDatabases", None) ::
      Seq("afterListDatabases", None) :: Nil)

    assert(catalog.listDatabases("wyeruwi") === Nil)
    checkEvents(
      Seq("beforeListDatabases", Some("wyeruwi")) ::
      Seq("afterListDatabases", Some("wyeruwi")) :: Nil)

    // ALTER
    catalog.alterDatabase(dbDefinition)
    checkEvents(
      Seq("beforeAlterDatabase", "db5") ::
      Seq("afterAlterDatabase", "db5") :: Nil)

    intercept[NoSuchDatabaseException] {
      catalog.alterDatabase(dbDefinition.copy(name = "db4"))
    }
    checkEvents(Seq("beforeAlterDatabase", "db4") :: Nil)

    // RENAME (NOT IMPLEMENTED FOR DATABASES)

    // EXISTS
    assert(catalog.databaseExists("db5"))
    checkEvents(
      Seq("beforeExistsDatabase", "db5") ::
      Seq("afterExistsDatabase", "db5") :: Nil)
    assert(!catalog.databaseExists("db4"))
    checkEvents(
      Seq("beforeExistsDatabase", "db4") ::
      Seq("afterExistsDatabase", "db4") :: Nil)

    // GET
    assert(catalog.getDatabase("db5") === dbDefinition)
    checkEvents(
      Seq("beforeGetDatabase", "db5") ::
      Seq("afterGetDatabase", "db5") :: Nil)
    intercept[NoSuchDatabaseException] {
      catalog.getDatabase("db4")
    }
    checkEvents(Seq("beforeGetDatabase", "db4") :: Nil)

    // GET OPTION (NOT IMPLEMENTED FOR DATABASES)

    // DROP
    intercept[NoSuchDatabaseException] {
      catalog.dropDatabase("db4", ignoreIfNotExists = false, cascade = false)
    }
    checkEvents(Seq("beforeDropDatabase", "db4") :: Nil)

    catalog.dropDatabase("db5", ignoreIfNotExists = false, cascade = false)
    checkEvents(
      Seq("beforeDropDatabase", "db5") ::
      Seq("afterDropDatabase", "db5") :: Nil)

    catalog.dropDatabase("db4", ignoreIfNotExists = true, cascade = false)
    checkEvents(Nil)
  }

  testWithCatalog("table") { (catalog, checkEvents) =>
    val path1 = Files.createTempDirectory("db_")
    val path2 = Files.createTempDirectory(path1, "tbl_")
    val uri1 = preparePath(path1)
    val uri2 = preparePath(path2)

    // CREATE
    val dbDefinition = createDbDefinition(uri1)

    val storage = CatalogStorageFormat.empty.copy(
      locationUri = Option(uri2))
    val tableDefinition = CatalogTable(
      identifier = TableIdentifier("tbl1", Some("db5")),
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = new StructType().add("id", "long"))

    val newIdentifier = tableDefinition.identifier.copy(table = "tbl2")
    val newUri = s"${path1.toFile.toURI.toString.stripSuffix("/")}/tbl2"
    val renamedTableDefinition = tableDefinition.copy(
      identifier = newIdentifier,
      storage = storage.copy(locationUri = Some(newUri)))

    catalog.createDatabase(dbDefinition, ignoreIfExists = false)

    checkEvents(
      Seq("beforeCreateDatabase", "db5") ::
      Seq("afterCreateDatabase", "db5") :: Nil)

    catalog.createTable(tableDefinition, ignoreIfExists = false)
    checkEvents(
      Seq("beforeCreateTable", "db5", "tbl1") ::
      Seq("afterCreateTable", "db5", "tbl1") :: Nil)

    catalog.createTable(tableDefinition, ignoreIfExists = true)
    checkEvents(Nil)

    intercept[TableAlreadyExistsException] {
      catalog.createTable(tableDefinition, ignoreIfExists = false)
    }
    checkEvents(Seq("beforeCreateTable", "db5", "tbl1") :: Nil)

    // LIST
    assert(catalog.listTables("db5") === Seq("tbl1"))
    checkEvents(
      Seq("beforeListTables", "db5", None) ::
      Seq("afterListTables", "db5", None) :: Nil)

    assert(catalog.listTables("db5", "wyeruwi") === Nil)
    checkEvents(
      Seq("beforeListTables", "db5", Some("wyeruwi")) ::
      Seq("afterListTables", "db5", Some("wyeruwi")) :: Nil)

    // ALTER
    catalog.alterTable(tableDefinition)
    checkEvents(
      Seq("beforeAlterTable", "db5", "tbl1") ::
      Seq("afterAlterTable", "db5", "tbl1") :: Nil)

    intercept[NoSuchTableException] {
      catalog.alterTable(renamedTableDefinition)
    }
    checkEvents(Seq("beforeAlterTable", "db5", "tbl2") :: Nil)

    // RENAME
    catalog.renameTable("db5", "tbl1", "tbl2")
    checkEvents(
      Seq("beforeRenameTable", "db5", "tbl1", "tbl2") ::
      Seq("afterRenameTable", "db5", "tbl1", "tbl2") :: Nil)

    intercept[NoSuchTableException] {
      catalog.renameTable("db5", "tbl1", "tbl2")
    }
    checkEvents(Seq("beforeRenameTable", "db5", "tbl1", "tbl2") :: Nil)

    // EXISTS
    assert(catalog.tableExists("db5", "tbl2"))
    checkEvents(
      Seq("beforeExistsTable", "db5", "tbl2") ::
      Seq("afterExistsTable", "db5", "tbl2") :: Nil)
    assert(!catalog.tableExists("db5", "tbl1"))
    checkEvents(
      Seq("beforeExistsTable", "db5", "tbl1") ::
      Seq("afterExistsTable", "db5", "tbl1") :: Nil)

    // GET
    assert(catalog.getTable("db5", "tbl2") === renamedTableDefinition)
    checkEvents(
      Seq("beforeGetTable", "db5", "tbl2") ::
      Seq("afterGetTable", "db5", "tbl2") :: Nil)
    intercept[NoSuchTableException] {
      catalog.getTable("db5", "tbl1")
    }
    checkEvents(Seq("beforeGetTable", "db5", "tbl1") :: Nil)

    // GET OPTION
    assert(catalog.getTableOption("db5", "tbl2") === Option(renamedTableDefinition))
    checkEvents(
      Seq("beforeGetTable", "db5", "tbl2") ::
      Seq("afterGetTable", "db5", "tbl2") :: Nil)
    assert(catalog.getTableOption("db5", "tbl1") === None)
    checkEvents(
      Seq("beforeGetTable", "db5", "tbl1") ::
      Seq("afterGetTable", "db5", "tbl1") :: Nil)

    // DROP
    intercept[NoSuchTableException] {
      catalog.dropTable("db5", "tbl1", ignoreIfNotExists = false, purge = true)
    }
    checkEvents(Seq("beforeDropTable", "db5", "tbl1") :: Nil)

    catalog.dropTable("db5", "tbl2", ignoreIfNotExists = false, purge = true)
    checkEvents(
      Seq("beforeDropTable", "db5", "tbl2") ::
      Seq("afterDropTable", "db5", "tbl2") :: Nil)

    catalog.dropTable("db5", "tbl2", ignoreIfNotExists = true, purge = true)
    checkEvents(Nil)
  }

  testWithCatalog("function") { (catalog, checkEvents) =>
    // CREATE
    val dbDefinition = createDbDefinition()

    val functionDefinition = CatalogFunction(
      identifier = FunctionIdentifier("fn7", Some("db5")),
      className = "",
      resources = Seq.empty)

    val newIdentifier = functionDefinition.identifier.copy(funcName = "fn4")
    val renamedFunctionDefinition = functionDefinition.copy(identifier = newIdentifier)

    catalog.createDatabase(dbDefinition, ignoreIfExists = false)
    checkEvents(
      Seq("beforeCreateDatabase", "db5") ::
        Seq("afterCreateDatabase", "db5") :: Nil)

    catalog.createFunction("db5", functionDefinition)
    checkEvents(
      Seq("beforeCreateFunction", "db5", "fn7") ::
        Seq("afterCreateFunction", "db5", "fn7") :: Nil)

    intercept[FunctionAlreadyExistsException] {
      catalog.createFunction("db5", functionDefinition)
    }
    checkEvents(Seq("beforeCreateFunction", "db5", "fn7") :: Nil)

    // LIST
    assert(catalog.listFunctions("db5", "f*") === Seq("fn7"))
    checkEvents(
      Seq("beforeListFunctions", "db5", Some("f*")) ::
        Seq("afterListFunctions", "db5", Some("f*")) :: Nil)

    // ALTER (NOT IMPLEMENTED FOR FUNCTIONS)

    // RENAME
    catalog.renameFunction("db5", "fn7", "fn4")
    checkEvents(
      Seq("beforeRenameFunction", "db5", "fn7", "fn4") ::
        Seq("afterRenameFunction", "db5", "fn7", "fn4") :: Nil)

    intercept[NoSuchFunctionException] {
      catalog.renameFunction("db5", "fn7", "fn4")
    }
    checkEvents(Seq("beforeRenameFunction", "db5", "fn7", "fn4") :: Nil)

    // EXISTS
    assert(catalog.functionExists("db5", "fn4"))
    checkEvents(
      Seq("beforeExistsFunction", "db5", "fn4") ::
      Seq("afterExistsFunction", "db5", "fn4") :: Nil)
    assert(!catalog.functionExists("db5", "fn7"))
    checkEvents(
      Seq("beforeExistsFunction", "db5", "fn7") ::
      Seq("afterExistsFunction", "db5", "fn7") :: Nil)

    // GET
    assert(catalog.getFunction("db5", "fn4") === renamedFunctionDefinition)
    checkEvents(
      Seq("beforeGetFunction", "db5", "fn4") ::
      Seq("afterGetFunction", "db5", "fn4") :: Nil)
    intercept[NoSuchFunctionException] {
      catalog.getFunction("db5", "fn7")
    }
    checkEvents(Seq("beforeGetFunction", "db5", "fn7") :: Nil)

    // GET OPTION (NOT IMPLEMENTED FOR FUNCTIONS)

    // DROP
    intercept[NoSuchFunctionException] {
      catalog.dropFunction("db5", "fn7")
    }
    checkEvents(Seq("beforeDropFunction", "db5", "fn7") :: Nil)

    catalog.dropFunction("db5", "fn4")
    checkEvents(
      Seq("beforeDropFunction", "db5", "fn4") ::
      Seq("afterDropFunction", "db5", "fn4") :: Nil)
  }
}
