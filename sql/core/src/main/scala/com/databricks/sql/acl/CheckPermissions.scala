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

import org.apache.spark.sql.catalog.{Catalog => PublicCatalog}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogTable}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.command.{PersistedView, _}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.python.PythonUDF
import org.apache.spark.sql.execution.streaming._

/**
 * Checks if the underlying principal has permissions to execute a logical plan
 * @param catalog Public catalog for object resolution
 * @param aclClient Client to the ACL store.
 */
class CheckPermissions(catalog: PublicCatalog, aclClient: AclClient)
    extends (LogicalPlan => Unit) {
  val permissionChecker = new PermissionChecker(aclClient)

  def apply(plan: LogicalPlan): Unit = {
    if (CheckPermissions.isTrusted) {
      return
    }
    getRequestsToCheck(plan).foreach { request =>
      if (!permissionChecker(request)) {
        throw new SecurityException(toErrorMessageForFailedRequest(request))
      }
    }
  }

  /**
   * Converts a logical plan or command to a sequence of trees of requests.
   */
  def getRequestsToCheck(plan: LogicalPlan): Seq[Request] = plan match {
    case command: Command => commandToRequests(command)
    case insert: InsertIntoTable =>
      queryToRequests(insert.table).map(_.copy(action = Modify)) ++
        queryToRequests(insert.child)
    case ExplainNode(query) =>
      toExplainRequests(getRequestsToCheck(query))
    case _ => queryToRequests(plan)
  }

  /**
   * Converts an expression tree to a sequence of requests. trees may be produced by
   * views in subquery expressions.
   */
  def toRequests(expression: Expression): Seq[Request] = {
    expression.children.flatMap(toRequests) ++ exprNodeToRequests(expression)
  }

  protected def isTempDb(db: Option[String]): Boolean = {
    /** TODO(srinath): Check for global temp database here */
    db.isEmpty
  }

  /**
   * Converts a query (i.e. not a command) to a tree of requests. Views induce trees
   * on their constituents. It can be assumed that the objects in this tree have been
   * resolved.
   */
  protected def queryToRequests(query: LogicalPlan): Seq[Request] = {
    def childRequests = query.expressions.flatMap(_.flatMap(toRequests)) ++
      query.children.flatMap(queryToRequests)
    query match {
      /** View references must nest their constituents */
      case SubqueryAlias(alias, childPlan, Some(view)) if !isTempDb(view.database) =>
        Seq(Request(Table(view), Select, queryToRequests(childPlan).toSet))

      /** Base table types */
      case rel: CatalogRelation if isTempDb(rel.catalogTable.identifier.database) => Nil
      case rel: CatalogRelation => Seq(Request(Table(rel.catalogTable.identifier), Select))

      case LogicalRelation(_: HadoopFsRelation, _, None) =>
        Seq(Select(AnyFile))
      case LogicalRelation(_, _, None) => Nil
      case LogicalRelation(_, _, Some(tableId)) if isTempDb(tableId.identifier.database) => Nil
      case LogicalRelation(_, _, Some(tableId)) =>
        Seq(Request(Table(tableId.identifier), Select))

      /** Operators that involve executing funcs that are not expressions */
      case _: AppendColumns => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: MapGroups => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: AppendColumnsWithObject => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: MapElements => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: MapPartitions => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: MapPartitionsInR => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: FlatMapGroupsInR => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: CoGroup => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: ScriptTransformation => Request(AnonymousFunction, Action.Select) +: childRequests
      case _: TypedFilter => Request(AnonymousFunction, Action.Select) +: childRequests

      /**
       * By default, the permissions required for an operator are just the permissions required
       * for its children.
       */
      case _ if alwaysPermit(query) => childRequests
      case _ => throw new SecurityException(s"Could not verify permissions for " +
        s"logical node ${query.simpleString}")

    }
  }

  /**
   * It is assumed that the objects in the command node (i.e. the root node of
   * param command) are not resolved, but any query trees that are children of the
   * command have been resolved.
   */
  protected def commandToRequests(command: Command): Seq[Request] = command match {
    case insert: InsertIntoHadoopFsRelationCommand =>
      toInserts(insert.catalogTable) ++ queryToRequests(insert.query)
    case insert: InsertIntoDataSourceCommand =>
      toInserts(insert.logicalRelation.catalogTable) ++ queryToRequests(insert.query)

    case load: LoadDataCommand =>
      val securables = toSecurables(load.table)
      val inserts = securables.map(toInsert)
      if (load.isOverwrite) {
        inserts ++ securables.map(toDelete)
      } else {
        inserts
      }

    case create: CreateTable =>
      toSecurables(create.tableDesc.identifier.database).map(Create) ++
        create.query.toSeq.flatMap(queryToRequests)
    case _: CreateTempViewUsing =>
      Seq(Select(AnyFile))
    case create: CreateDataSourceTableCommand =>
      toSecurables(create.table.identifier.database).map(Create)
    case create: CreateTableCommand =>
      toSecurables(create.table.identifier.database).map(Create)
    case create: CreateViewCommand if create.viewType == PersistedView =>
      toSecurables(create.name.database).map(Create)
    case create: CreateViewCommand => Nil
    case create: CreateTableLikeCommand =>
      toSecurables(create.targetTable.database).map(Create) ++
        toSecurables(create.sourceTable).map(ReadMetadata)

    /** Alter table partitions */
    case alter: AlterTableAddPartitionCommand =>
      toSecurables(alter.tableName).map(toInsert)
    case alter: AlterTableDropPartitionCommand =>
      toSecurables(alter.tableName).map(toDelete)
    case alter: AlterTableRenamePartitionCommand =>
      toSecurables(alter.tableName).map(Modify)

    /** Do we need create on the database for table rename */
    case alter: AlterTableRenameCommand =>
      toSecurables(alter.oldName).map(Own)
    case alter: AlterTableRecoverPartitionsCommand =>
      toSecurables(alter.tableName).map(Own)
    case alter: AlterTableSerDePropertiesCommand =>
      toSecurables(alter.tableName).map(Own)
    case alter: AlterTableSetPropertiesCommand =>
      toSecurables(alter.tableName).map(Own)
    case alter: AlterTableUnsetPropertiesCommand =>
      toSecurables(alter.tableName).map(Own)
    case alter: AlterTableSetLocationCommand =>
      toSecurables(alter.tableName).map(Own)

    case explain: DescribeTableCommand =>
      toSecurables(explain.table).map(ReadMetadata)
    case explain: ShowTablePropertiesCommand =>
      toSecurables(explain.table).map(ReadMetadata)

    case drop: DropTableCommand =>
      toSecurables(drop.tableName).map(Own)

    case truncate: TruncateTableCommand =>
      toSecurables(truncate.tableName).map(toDelete)
    case analyze: AnalyzeTableCommand =>
      toSecurables(analyze.tableIdent).flatMap { securable =>
        Seq(toInsert(securable), Select(securable))
      }

    case describe: DescribeFunctionCommand =>
      toSecurables(describe.functionName).map(ReadMetadata)

    case create: CreateFunctionCommand if create.isTemp => Nil
    case create: CreateFunctionCommand =>
      toSecurables(create.databaseName).map(CreateNamedFunction)

    /** Need to re-check on actual drop */
    case drop: DropFunctionCommand if drop.isTemp => Nil
    case drop: DropFunctionCommand =>
      toSecurables(FunctionIdentifier(drop.functionName, drop.databaseName)).map(Own)

    case _: ShowDatabasesCommand =>
      Seq(Request(Catalog, ReadMetadata))
    case show: ShowTablesCommand =>
      toSecurables(show.databaseName).map(ReadMetadata)
    case show: ShowFunctionsCommand =>
      toSecurables(show.db).map(ReadMetadata)
    case show: ShowPartitionsCommand =>
      toSecurables(show.tableName).map(ReadMetadata)
    case show: ShowColumnsCommand =>
      toSecurables(show.tableName).map(ReadMetadata)
    case show: ShowCreateTableCommand =>
      toSecurables(show.table).map(ReadMetadata)

    case create: CreateDatabaseCommand => Seq(Request(Catalog, Create))
    case alter: AlterDatabasePropertiesCommand =>
      Seq(Request(Database(alter.databaseName), Own))
    case drop: DropDatabaseCommand =>
      Seq(Request(Database(drop.databaseName), Own))

    case _: AddFileCommand | _: AddJarCommand =>
      Seq(Request(Catalog, ModifyClasspath))

    case ManagePermissions(permissions) =>
      permissions.map(_.securable).flatMap(toResolvedSecurables).map(Own)

    case ChangeOwnerCommand(_, securable, _) =>
      toResolvedSecurables(securable).map(Own)
    case CleanPermissionsCommand(_, securable) =>
      toResolvedSecurables(securable).map(Own)

    case vacuum: VacuumTableCommand if vacuum.table.isDefined =>
      toSecurables(vacuum.table.get).map(toDelete)
    case vacuum: VacuumTableCommand if vacuum.path.isDefined =>
      Seq(Modify(AnyFile))

    case _ if alwaysPermit(command) => Nil
    case _ =>
      throw new SecurityException(s"Could not verify permissions for ${command.simpleString}")
    }

  private def alwaysPermit(plan: LogicalPlan): Boolean = plan match {
    case _: StreamingExecutionRelation => true
    case _: MemoryPlan => true
    case _: LocalRelation => true
    case OneRowRelation => true
    case _: DeserializeToObject => true
    case _: Filter => true
    case _: Distinct => true
    case _: BroadcastHint => true
    case _: Project => true
    case _: Window => true
    case _: GlobalLimit => true
    case _: SerializeFromObject => true
    case _: Aggregate => true
    case _: RepartitionByExpression => true
    case _: SubqueryAlias => true
    case _: Expand => true
    case _: LocalLimit => true
    case _: Sort => true
    case _: Repartition => true
    case _: Sample => true
    case _: Join => true
    case _: Union => true
    case _: Intersect => true
    case _: Except => true
    case _: LogicalRDD => true
    case ClearCacheCommand => true
    case _: UncacheTableCommand => true
    case _: RefreshTable => true
    case _: RefreshResource => true
    case _: SetCommand => true
    case _: SetDatabaseCommand => true
    case ResetCommand => true
    case _: ListFilesCommand => true
    case  _: ListJarsCommand => true
    case _: Range => true
    case _: Generate => true
    case _: ExplainCommand => true
    case NoOpRunnableCommand => true
    case _: ShowPermissionsCommand => true
    case _ => false
  }

  /**
   * Return permission requests required for an expression only, NOT including the subtree
   * rooted at that expression
   */
  protected def exprNodeToRequests(expr: Expression): Seq[Request] = expr match {
    case subQuery: SubqueryExpression =>
      queryToRequests(subQuery.plan)
    case _: ScalaUDF | _: ScalaUDAF | _: UserDefinedGenerator |
         _: PythonUDF | _: TypedAggregateExpression |
         _: CallMethodViaReflection =>
      Seq(Request(AnonymousFunction, Select))
    /**
     * TODO(srinath): Maybe these functions should be disallowed ? If not, requiring anonymous
     * function permissions is the next best thing.
     */
    case InputFileName() | SparkPartitionID() => Seq(Request(AnonymousFunction, Select))
    case _ => Seq.empty[Request]
  }

  /**
   * The toSecurables functions convert a database, table/views or function to the
   * corresponding securables. Temporary tables and functions are not securable.
   */
  protected def toSecurables(database: Option[String]): Seq[Securable] = {
    val name = database match {
      case None => catalog.currentDatabase
      case Some(db) =>
        catalog.getDatabase(db)
        db
    }
    Seq(Database(name))
  }

  protected def toSecurables(unresolved: TableIdentifier): Seq[Securable] = {
    val resolved = catalog.getTable(unresolved.database.orNull, unresolved.table)
    assert(resolved.isTemporary || resolved.database != null)
    if (resolved.isTemporary) {
      Nil
    } else {
      Seq(Table(TableIdentifier(resolved.name, Some(resolved.database))))
    }
  }

  protected def toSecurables(unresolved: FunctionIdentifier): Seq[Securable] = {
    val resolved = catalog.getFunction(unresolved.database.orNull, unresolved.funcName)
    assert(resolved.isTemporary || resolved.database != null)
    if (resolved.isTemporary) {
      Nil
    } else {
      Seq(Function(FunctionIdentifier(resolved.name, Some(resolved.database))))
    }
  }

  protected def toResolvedSecurables(securable: Securable): Seq[Securable] = securable match {
    case Table(unresolved) => toSecurables(unresolved)
    case Function(unresolved) => toSecurables(unresolved)
    case _ => Seq(securable)
  }

  private object ManagePermissions {
    def unapply(command: AclCommand): Option[Seq[Permission]] = command match {
      case grant: GrantPermissionsCommand => Some(grant.permissions)
      case revoke: RevokePermissionsCommand => Some(revoke.permissions)
      case _ => None
    }
  }

  private def toInsert: Securable => Request = Modify

  private def toDelete: Securable => Request = Modify

  private def toInserts(table: Option[CatalogTable]): Seq[Request] = {
    table.toSeq.flatMap(t => toSecurables(t.identifier).map(toInsert))
  }

  /**
   * Change the requested permissions to [[ReadMetadata]] if the user is calling explain
   * on the query.
   *
   * Note that we drop actions on [[AnonymousFunction]]s because [[ReadMetadata]] does
   * not make much sense there.
   */
  private def toExplainRequests(requests: Seq[Request]): Seq[Request] = {
    requests.collect {
      case Request(securable, _, children) if securable != AnonymousFunction =>
        Request(securable, ReadMetadata, toExplainRequests(children.toSeq).toSet)
    }
  }

  private def toErrorMessageForFailedRequest(request: Request): String = {
    val actionString = request.action match {
      case Own => "own"
      case _ => s"have permission ${request.action} on"
    }

    s"User does not ${actionString} ${request.securable.prettyString}"
  }
}

private[databricks] object CheckPermissions {
  private val trusted = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  private[databricks] def isTrusted: Boolean = trusted.get()

  private[databricks] def trusted[T](block: => T): T = {
    trusted.set(true)
    try block finally {
      trusted.set(false)
    }
  }
}
