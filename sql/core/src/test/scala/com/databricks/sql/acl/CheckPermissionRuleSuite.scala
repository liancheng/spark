/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import com.databricks.sql.acl.Action.{ReadMetadata, Select}
import org.apache.hadoop.fs.Path
import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalog.{Catalog => PublicCatalog, Table => PublicTable}
import org.apache.spark.sql.catalog.{Function => PublicFunction}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.python.PythonUDF
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{StringType, StructType}

trait CheckRequests extends SparkFunSuite {
  val catalog: PublicCatalog = mock(classOf[PublicCatalog])
  val checkRule: CheckPermissions = new CheckPermissions(catalog, null)

  def checkAnswer(tree: LogicalPlan, expected: Set[Request], testName: String = "")
  : Unit = {
    val actual = checkRule.getRequestsToCheck(tree).toSet
    if (actual != expected) {
      val diff = actual ++ expected -- actual.intersect(expected)
      fail(
        s"""$testName
           |===== TREE =====
           |$tree
           |=== EXPECTED ===
           |${expected.mkString("\n")}
           |=== RETURNED ===
           |${actual.mkString("\n")}
           |===== DIFF =====
           |${diff.mkString("\n")}
           |""".stripMargin)
    }
  }
}

class CheckPermissionRuleSuite extends CheckRequests {

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(catalog.getTable(tableName = T1.table, dbName = T1.database.orNull))
      .thenReturn(new PublicTable(T1.table, T1.database.orNull, null, null, false))
    when(catalog.getTable(tableName = T2.table, dbName = T2.database.orNull))
      .thenReturn(new PublicTable(T2.table, T2.database.orNull, null, null, false))
    when(catalog.getTable(tableName = V1.table, dbName = V1.database.orNull))
      .thenReturn(new PublicTable(V1.table, V1.database.orNull, null, null, false))
    when(catalog.getTable(tableName = temp1.table, dbName = temp1.database.orNull))
      .thenReturn(new PublicTable(temp1.table, null, null, null, true))
    when(catalog.getTable(tableName = tempV1.table, dbName = tempV1.database.orNull))
      .thenReturn(new PublicTable(tempV1.table, null, null, null, true))
    when(catalog.getFunction(functionName = func1.funcName, dbName = func1.database.orNull))
      .thenReturn(new PublicFunction(func1.funcName, func1.database.orNull, null, null, false))
    when(catalog.getFunction(functionName = tempFunc1.funcName,
        dbName = tempFunc1.database.orNull))
      .thenReturn(new PublicFunction(tempFunc1.funcName, tempFunc1.database.orNull, null,
        null, true))

  }

  protected def createCatalogTable(id: TableIdentifier): CatalogTable = {
    CatalogTable(
      identifier = id,
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("id", "long"),
      provider = Option("parquet"))
  }


  protected def toRel(id: TableIdentifier): SimpleCatalogRelation = {
    SimpleCatalogRelation(
      databaseName = id.database.orNull,
      metadata = createCatalogTable(id))
  }

  protected def tempToRel(id: String): LogicalRelation = {
    val mockBaseRel = mock(classOf[BaseRelation])
    when(mockBaseRel.schema).thenReturn(new StructType)
    LogicalRelation(mockBaseRel, None, Some(createCatalogTable(temp1)))
  }

  protected def anonymousRel(): LogicalRelation = {
    val mockBaseRel = mock(classOf[HadoopFsRelation])
    when(mockBaseRel.schema).thenReturn(new StructType)
    LogicalRelation(mockBaseRel, None, None)
  }

  val db: String = "PERM1"
  val V1: TableIdentifier = TableIdentifier("V1", Some(db))
  val T1: TableIdentifier = TableIdentifier("T1", Some(db))
  val T2: TableIdentifier = TableIdentifier("T2", Some(db))
  val temp1: TableIdentifier = TableIdentifier("temp1", None)
  val tempV1: TableIdentifier = TableIdentifier("tempV1", None)
  val func1: FunctionIdentifier = FunctionIdentifier("func", Some(db))
  val tempFunc1: FunctionIdentifier = FunctionIdentifier("tempfunc", None)
  val anonymousExpr: Expression = ScalaUDF(null, StringType, Nil)
  val subqueryExpr: Expression = ScalarSubquery(toRel(T1))

  private def add(name: String, builder: LogicalPlan => LogicalPlan, exprs: Expression*)
     : (String, LogicalPlan => LogicalPlan, Seq[Expression]) = {
    (name, builder, exprs.toSeq)
  }

  private def addC(name: String, builder: LogicalPlan => LogicalPlan, requests: Request*)
      : (String, LogicalPlan => LogicalPlan, Set[Request]) = {
    (name, builder, requests.toSet)
  }

  private def addDDL(name: String, plan: LogicalPlan, requests: Request*)
      : (String, LogicalPlan, Set[Request]) = {
    (name, plan, requests.toSet)
  }
  private val passThroughPlans: Seq[(String, LogicalPlan => LogicalPlan, Seq[Expression])] = Seq(
    add("ScriptTransformation",
      plan => ScriptTransformation(Seq(subqueryExpr), null, null, plan, null),
      subqueryExpr, anonymousExpr),
    add("Generate",
      plan => Generate(UserDefinedGenerator(new StructType, _ => Iterator.empty, Seq.empty),
        true, false, None, Seq.empty, plan), anonymousExpr),
    add("MapGroups",
      plan => MapGroups(null, anonymousExpr, Literal(1), Nil, Nil, null, plan), anonymousExpr),
    add("FlatMapGroupsInR",
      plan => FlatMapGroupsInR(Array.empty, Array.empty, Array.empty,
        new StructType, new StructType, Literal(1), Literal(1), Seq.empty, Seq.empty,
        UnresolvedAttribute("a"), plan), anonymousExpr),
    add("MapElements", plan => MapElements(
      null, classOf[Int], new StructType, UnresolvedAttribute("a"), plan), anonymousExpr),
    add("MapPartitions", plan => MapPartitions(null, UnresolvedAttribute("a"), plan),
      anonymousExpr),
    add("MapPartitionsInR",
      plan => MapPartitionsInR(Array.empty, Array.empty, Array.empty,
        new StructType, new StructType, UnresolvedAttribute("a"), plan), anonymousExpr),
    add("AppendColumns",
      plan => AppendColumns(
        null, classOf[Int], new StructType, Literal(1), Seq.empty, plan), anonymousExpr),
    add("CoGroup",
      plan => CoGroup(null, Literal(1), Literal(1), Literal(1), Seq.empty, Seq.empty,
        Seq.empty, Seq.empty, UnresolvedAttribute("a"), plan, plan), anonymousExpr),
    add("TypedFilter",
      plan => TypedFilter(null, classOf[Int], new StructType, Literal(1), plan), anonymousExpr),
    add("Filter", plan => Filter(anonymousExpr, plan), anonymousExpr),
    add("Distinct", plan => Distinct(plan)),
    add("BroadCastHint", plan => BroadcastHint(plan)),
    add("Project", plan => Project(Seq(Alias(anonymousExpr, "a")(), Alias(subqueryExpr, "a")()),
        plan), anonymousExpr, subqueryExpr),
    add("Window", plan => Window(Seq(Alias(anonymousExpr, "a")()),
        Seq(Alias(subqueryExpr, "a")()), Seq.empty, plan), anonymousExpr, subqueryExpr),
    add("Limit", plan => Limit(anonymousExpr, plan), anonymousExpr),
    add("Aggregate", plan => Aggregate(Seq(subqueryExpr), Seq(Alias(anonymousExpr, "a")()), plan),
      subqueryExpr, anonymousExpr),
    add("RepartitionByExpression", plan => RepartitionByExpression(Seq(subqueryExpr), plan),
      subqueryExpr),
    add("SubqueryAlias", plan => SubqueryAlias("a", plan, None)),
    add("Expand", plan => Expand(Seq.empty, Seq.empty, plan)),
    add("Sort", plan => Sort(Seq(SortOrder(subqueryExpr, Ascending)), true, plan), subqueryExpr),
    add("Repartition", plan => Repartition(10, true, plan)),
    add("Sample", plan => Sample(0.0, 0.0, false, 0, plan)()),
    add("Join", plan => Join(plan, plan, JoinType("inner"), Some(subqueryExpr)), subqueryExpr),
    add("Union", plan => Union(plan, plan)),
    add("Intersect", plan => Intersect(plan, plan)),
    add("Except", plan => Except(plan, plan)))

  private val passThroughCommands: Seq[(String, LogicalPlan => LogicalPlan, Set[Request])] = Seq(
    addC("InsertIntoTable", plan => InsertIntoTable(
      toRel(T1), Map.empty, plan, OverwriteOptions(false), false),
      Request(Table(T1), Action.Modify)),
    addC("InsertFs", plan => InsertIntoHadoopFsRelationCommand(
      new Path("x"), Map.empty, Map.empty,
      Seq.empty, None, new TextFileFormat, _ => (),
      Map.empty, plan, SaveMode.Append, Some(createCatalogTable(T1))),
      Request(Table(T1), Action.Modify)),
    addC("InsertDS", plan => InsertIntoDataSourceCommand(
      mockedLogicalRelation(T1),
      plan,
      OverwriteOptions(enabled = false)),
      Request(Table(T1), Action.Modify)),
    addC("CTAS", plan => CreateTable(
      createCatalogTable(T1), SaveMode.ErrorIfExists, Option(plan)),
      Request(Database(T1.database.get), Action.Create)))

  private val ddlOnlyCommands: Seq[(String, LogicalPlan, Set[Request])] = Seq(
    addDDL("create temp view", CreateTempViewUsing(
      TableIdentifier("blah"), None, false, false, null, Map.empty),
      Request(AnyFile, Action.Select)),
    addDDL("createdstable", CreateDataSourceTableCommand(
      createCatalogTable(T1), false),
      Request(Database(T1.database.get), Action.Create)),
    addDDL("create table", CreateTableCommand(createCatalogTable(T1), false),
      Request(Database(T1.database.get), Action.Create)),
    addDDL("create table like", CreateTableLikeCommand(T1, T2, false),
      Request(Database(T1.database.get), Action.Create), Request(Table(T2), Action.ReadMetadata)),
    addDDL("alterset", AlterTableSetPropertiesCommand(T1, Map.empty, false),
      Request(Table(T1), Action.Own)),
    addDDL("alterunset", AlterTableUnsetPropertiesCommand(T1, Nil, false, false),
      Request(Table(T1), Action.Own)),
    addDDL("altersetloc", AlterTableSetLocationCommand(T1, None, ""),
      Request(Table(T1), Action.Own), Request(AnyFile, Action.Select),
      Request(AnyFile, Action.Modify)),
    addDDL("alterrename", AlterTableRenameCommand(T1, T2, false),
      Request(Table(T1), Action.Own)),
    addDDL("alterserde", AlterTableSerDePropertiesCommand(T1, Some("some"), None, None),
      Request(Table(T1), Action.Own)),
    addDDL("describe", DescribeTableCommand(T1, Map.empty, false, false),
      Request(Table(T1), Action.ReadMetadata)),
    addDDL("showtabprop", ShowTablePropertiesCommand(T1, None),
      Request(Table(T1), Action.ReadMetadata)),
    addDDL("droptable", DropTableCommand(T1, false, false, false),
      Request(Table(T1), Action.Own)),
    addDDL("truncate", TruncateTableCommand(T1, None), Request(Table(T1), Action.Modify)),
    addDDL("vacuumTable", VacuumTableCommand(None, Some(T1), None),
      Request(Table(T1), Action.Modify)),
    addDDL("vacuumPath", VacuumTableCommand(Some("/foo"), None, None),
      Request(AnyFile, Action.Modify)),
    addDDL("analyze", AnalyzeTableCommand(T1),
      Request(Table(T1), Action.Modify), Request(Table(T1), Action.Select)),
    addDDL("loaddata", LoadDataCommand(T1, "blah", false, false, None),
      Request(Table(T1), Action.Modify)),
    addDDL("createfunc", CreateFunctionCommand(None, "func", "", Nil, true)),
    addDDL("createfunctemp", CreateFunctionCommand(func1.database, func1.funcName, "", Nil, false),
      Request(Database(func1.database.orNull), Action.CreateNamedFunction)),
    addDDL("descfunc", DescribeFunctionCommand(func1, false),
      Request(Function(func1), Action.ReadMetadata)),
    addDDL("descfunctemp", DescribeFunctionCommand(tempFunc1, false)),
    addDDL("dropfunc", DropFunctionCommand(func1.database, func1.funcName, false, false),
      Request(Function(func1), Action.Own)),
    addDDL("dropfunctmp", DropFunctionCommand(tempFunc1.database, tempFunc1.funcName, false, true)),
    addDDL("noop", NoOpRunnableCommand),
    addDDL("showdatabases", ShowDatabasesCommand(None), ReadMetadata(Catalog)),
    addDDL("showtables", ShowTablesCommand(Option(db), None), ReadMetadata(Database(db))),
    addDDL("showfunctions", ShowFunctionsCommand(Option(db), None, true, true),
      ReadMetadata(Database(db))),
    addDDL("showpartitions", ShowPartitionsCommand(T1, None), ReadMetadata(Table(T1))),
    addDDL("showcolmns", ShowColumnsCommand(None, T1), ReadMetadata(Table(T1))),
    addDDL("showcreatetable", ShowCreateTableCommand(T1), ReadMetadata(Table(T1))),
    addDDL("showpermissions", ShowPermissionsCommand(NoOpAclClient, None, Catalog)),
    addDDL("logicalrelation", anonymousRel(), Select(AnyFile))
  )
  private def mockedLogicalRelation(tableId: TableIdentifier): LogicalRelation = {
    val mocked = mock(classOf[LogicalRelation])
    when(mocked.catalogTable).thenReturn(Some(createCatalogTable(tableId)))
    mocked
  }

  test("Single tables") {
    checkAnswer(toRel(T1), Set(Request(Table(T1), Action.Select)))
    checkAnswer(tempToRel(temp1.table), Set.empty[Request])
  }

  test("Simple perm view") {
    /* V1 = T1 JOIN T2 */
    val query = SubqueryAlias("blah", Join(toRel(T1), toRel(T2), JoinType("inner"), None),
      Some(V1))
    val expected =
      Request(Table(V1), Action.Select, Set(
        Request(Table(T1), Action.Select),
        Request(Table(T2), Action.Select)))
    checkAnswer(query, Set(expected))
  }

  test("Perm view over temp") {
    /* V1 = T1 union temp1 */
    val query = SubqueryAlias("blah", Union(toRel(T1), tempToRel(temp1.table)), Some(V1))
    val expected =
      Request(Table(V1), Action.Select, Set(
        Request(Table(T1), Action.Select)))
    checkAnswer(query, Set(expected))
  }

  test("Temp view over perm") {
    /* tempV1 = T1 union T2 */
    val query = SubqueryAlias("blah", Union(toRel(T1), toRel(T2)), Some(tempV1))
    val expected = Set(Request(Table(T1), Action.Select),
      Request(Table(T2), Action.Select))
    checkAnswer(query, expected)
  }

  test("Subquery expression") {
    /* T1 UNION T2 */
    val subquery = SubqueryAlias("blah", Union(toRel(T1), toRel(T2)), None)
    val query = Filter(ScalarSubquery(subquery), OneRowRelation)
    checkAnswer(query, Set(
      Request(Table(T1), Action.Select),
      Request(Table(T2), Action.Select)))
  }

  test("Anonymous functions") {
    val anonymous = Seq(
      ScalaUDF(null, StringType, Nil),
      UserDefinedGenerator(new StructType, _ => Iterator.empty, Seq.empty),
      PythonUDF("blah", null, StringType, Nil))
    anonymous.foreach { expr =>
      val query = Filter(expr, OneRowRelation)
      checkAnswer(query, Set(Request(AnonymousFunction, Action.Select)))
    }
  }

  test("Expression tree flattened") {
    val subquery = SubqueryAlias("blah", toRel(T1), None)
    val query =
      Filter(
        And(
          ScalarSubquery(subquery),
          ScalaUDF(null, StringType, Nil)),
        OneRowRelation)
    checkAnswer(query, Set(
      Request(Table(T1), Action.Select),
      Request(AnonymousFunction, Action.Select)))
  }

  test("Pass through operators") {
    val child = Project(Seq(Alias(Literal(1), "a")()),
      Filter(ScalarSubquery(toRel(V1)), toRel(T2)))
    val childRequests = checkRule.getRequestsToCheck(child).toSet
    passThroughPlans.foreach { planDesc =>
      val (planName, builder, expressions) = planDesc
      val plan = builder(child)
      checkAnswer(plan, childRequests ++ expressions.flatMap(checkRule.toRequests).toSet, planName)
    }
  }

  test("Pass through Commands") {
    val child = Project(Seq(Alias(Literal(1), "a")()),
      Filter(ScalarSubquery(toRel(V1)), toRel(T2)))
    val childRequests = checkRule.getRequestsToCheck(child).toSet
    passThroughCommands.foreach { planDesc =>
      val (planName, builder, modifyRequests) = planDesc
      val plan = builder(child)
      checkAnswer(plan, childRequests ++ modifyRequests, planName)
    }
  }

  test("Only DDL") {
    ddlOnlyCommands.foreach {commandDesc =>
      val (commandName, plan, modifyRequests) = commandDesc
      checkAnswer(plan, modifyRequests, commandName)
    }
  }

  test("Explain") {
    def toReadMetadata(requests: Set[Request]): Set[Request] = {
      requests.filter(_.securable != AnonymousFunction).map { request =>
        Request(request.securable, ReadMetadata, toReadMetadata(request.children))
      }
    }
    val child = Project(Seq(Alias(Literal(1), "a")()), OneRowRelation)
    passThroughCommands.foreach {
      case (planName, builder, modifyRequests) =>
        val plan = ExplainNode(builder(child))
        checkAnswer(plan, toReadMetadata(modifyRequests), planName)
    }
  }
}
