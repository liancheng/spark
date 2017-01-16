/*
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import scala.collection.mutable

import org.apache.spark.sql.catalyst.{FunctionIdentifier, IdentifierWithDatabase, TableIdentifier}

/**
 * Base abstraction for a Principal.
 */
trait Principal {
  def name: String
  override def toString: String = name
}

/**
 * A place holder for the current principal (user). This object should NEVER have any permissions
 * granted to it.
 */
case object CurrentPrincipal extends Principal {
  override def name: String = "CURRENT_PRINCIPAL"
}

/**
 * An unknown user. This should NEVER have any permissions granted to it.
 */
case object UnknownPrincipal extends Principal {
  override def name: String = "UNKNOWN"
}

/**
 * A user identified by his user name.
 */
case class NamedPrincipal(name: String) extends Principal

/**
 * A user that owns the given securable.
 */
case class Owner(securable: Securable) extends Principal {
  override def name: String = s"${securable}_OWNER"
}

/**
 * A [[Securable]] is an object which can be secured. Permissions can be granted, revoked and
 * enforced for these objects. A securable object should be owned by a principal.
 */
sealed trait Securable {
  val name: String
  val key: AnyRef
  def typeName: String = getClass.getSimpleName.toUpperCase
  def prettyString: String
}

object Securable {
  /**
   * Add backticks to a name part.
   */
  def addBackticks(raw: String): String = {
    "`" + raw.replace("`", "``") + "`"
  }

  /**
   * Parse a [[Securable]]'s string representation into the name parts. Name parts should
   * delimited by slashes `/`. Any character is allowed in a name part. Surround the name with
   * backticks if a name contains a slash '/', and make sure that any literal backtick is escaped
   * with another backtick. It is assumed that the name part is backticked if it starts with a
   * backtick.
   */
  def parseString(rep: String): Seq[String] = {
    val parts = mutable.Buffer.empty[String]
    val buffer = new StringBuilder
    var i = 0
    // The quoted flag indicates that the current name part was backticked at some point. This is
    // used to detect situations in which the name part is only partially backticked (which is not
    // allowed)
    var quoted = false
    // The backticked flag indicates that we are currently in backticked parsing mode. In
    // backticked mode slashes are allowed as part of the name and double backticks are converted
    // into a single backtick.
    var backticked = false
    val length = rep.length
    while (i < length) {
      rep(i) match {
        // Name parts should be separated by slashes
        case '/' if !backticked =>
          // Skip an initial '/'.
          if (i > 0) {
            parts += buffer.toString()
            buffer.clear()
          }
          quoted = false
        // Name is quoted.
        case '`' if buffer.isEmpty && !backticked =>
          quoted = true
          backticked = true
        // Escaped backtick.
        case '`' if backticked && i + 1 < length && rep(i + 1) == '`' =>
          buffer += '`'
          i += 1
        // End of backticked part.
        case '`' if backticked =>
          backticked = false
        // Add the character.
        case c =>
          // A quoted name part should not have any characters outside of the backticked name.
          if (quoted && !backticked) {
            throw new IllegalArgumentException(
              s"A backticked name part should not have characters outside " +
                s"of the surrounding backticks: ${i + 1} @ $rep")
          }
          buffer += c
      }
      i += 1
    }

    // Add the last part.
    if (backticked) {
      throw new IllegalArgumentException(
        s"A backticked name part should be terminated by a backtick: $rep")
    }
    if (buffer.nonEmpty) {
      parts += buffer.toString()
    }

    // Done.
    parts
  }

  /**
   * Convert a string into a securable.
   */
  def fromString(rep: String): Securable = {
    parseString(rep) match {
      case Seq("ANONYMOUS_FUNCTION") =>
        AnonymousFunction
      case Seq("ANY_FILE") =>
        AnyFile
      case Seq("CATALOG", "default") =>
        Catalog
      case Seq("CATALOG", "default", "DATABASE", db) =>
        Database(db)
      case Seq("CATALOG", "default", "DATABASE", db, "TABLE", table) =>
        Table(TableIdentifier(table, Some(db)))
      case Seq("CATALOG", "default", "DATABASE", db, "FUNCTION", function) =>
        Function(FunctionIdentifier(function, Some(db)))
      case parts =>
        throw new IllegalArgumentException(
          s"'$rep' cannot be converted into a Securable: $parts")
    }
  }
}

case object Catalog extends Securable {
  override val key: Option[Nothing] = None
  override val name: String = "/CATALOG/`default`"
  override def prettyString: String = "CATALOG"
}

case class Database(key: String) extends Securable {
  assert(key != null)
  override val name: String = {
    val name = Securable.addBackticks(key)
    s"${Catalog.name}/DATABASE/$name"
  }
  override def prettyString: String = s"database ${Securable.addBackticks(key)}"
}

sealed trait DatabaseSecurable extends Securable {
  override val key: IdentifierWithDatabase
  override lazy val name: String = {
    val db = Database(key.database.getOrElse {
      throw new IllegalStateException(s"A database is not defined for $typeName $key")
    })
    s"${db.name}/$typeName/${Securable.addBackticks(key.identifier)}"
  }
}

case class Table(key: TableIdentifier) extends DatabaseSecurable {
  assert(key != null)
  override def prettyString: String = s"table ${key.quotedString}"
}

object Table {
  def apply(db: String, table: String): Table = Table(TableIdentifier(table, Option(db)))
  def apply(table: String): Table = apply(null, table)
}

sealed trait SecurableFunction extends Securable

case class Function(key: FunctionIdentifier) extends DatabaseSecurable with SecurableFunction {
  assert(key != null)
  override def prettyString: String = s"function ${key.quotedString}"
}

object Function {
  def apply(db: String, fn: String): Function = Function(FunctionIdentifier(fn, Option(db)))
  def apply(fn: String): Function = apply(null, fn)
}

case object AnonymousFunction extends SecurableFunction {
  override val key: Option[Nothing] = None
  override def typeName: String = "ANONYMOUS_FUNCTION"
  override val name: String = "/" + typeName
  override def prettyString: String = s"anonymous function"
}

case object AnyFile extends Securable {
  override val key: Option[Nothing] = None
  override def typeName: String = "ANY_FILE"
  override val name: String = "/" + typeName
  override def prettyString: String = s"any file"
}

/**
 * The action that a [[Principal]] wants to execute on a [[Securable]]. An [[Action]] should always
 * be a verb.
 */
trait Action extends (Securable => Request) {
  /** Name of the action type. */
  val name: String

  /** Check if it is valid to request permission for action on the given securable. */
  def validateRequest(securable: Securable): Boolean

  /** Check if it is valid to grant permission for action on the given securable. */
  def validateGrant(securable: Securable): Boolean

  /** Create a securable action for the given securable. */
  def apply(securable: Securable): Request = Request(securable, this)

  /** Return the name. */
  override def toString: String = name
}

object Action {

  /** Own an object. */
  object Own extends Action {
    override val name: String = "OWN"
    override def validateRequest(securable: Securable): Boolean = true
    override def validateGrant(securable: Securable): Boolean = false
  }

  /** View an object and its metadata. */
  object ReadMetadata extends Action {
    override val name: String = "READ_METADATA"
    override def validateRequest(securable: Securable): Boolean = true
    override def validateGrant(securable: Securable): Boolean = true
  }

  /** Create a new database or table in an existing catalog or database respectively */
  object Create extends Action {
    override val name: String = "CREATE"

    override def validateRequest(securable: Securable): Boolean = securable match {
      case Catalog | _: Database => true
      case _ => false
    }

    override def validateGrant(securable: Securable): Boolean = securable match {
      case Catalog | _: Database => true
      case _ => false
    }
  }

  /** Create a named function in an existing catalog or database */
  object CreateNamedFunction extends Action {
    override val name: String = "CREATE_NAMED_FUNCTION"

    override def validateRequest(securable: Securable): Boolean = securable match {
      case Catalog | _: Database => true
      case _ => false
    }

    override def validateGrant(securable: Securable): Boolean = securable match {
      case Catalog | _: Database => true
      case _ => false
    }
  }

  /** Read data from an object or execute a function. */
  object Select extends Action {
    override val name: String = "SELECT"

    override def validateRequest(securable: Securable): Boolean = securable match {
      case _: Table | _: SecurableFunction | AnyFile => true
      case _ => false
    }

    override def validateGrant(securable: Securable): Boolean = securable match {
      case Catalog | _: Database | _: Table | _: SecurableFunction | AnyFile => true
      case _ => false
    }
  }

  /** Load/Insert/Update/Delete/Truncate data to/from an object. */
  object Modify extends Action {
    override val name: String = "MODIFY"

    override def validateRequest(securable: Securable): Boolean = securable match {
      case _: Table | AnyFile => true
      case _ => false
    }

    override def validateGrant(securable: Securable): Boolean = securable match {
      case Catalog | _: Database | _: Table | AnyFile => true
      case _ => false
    }
  }

  /** Load a resource on to the classpath */
  object ModifyClasspath extends Action {
    override val name: String = "MODIFY_CLASSPATH"
    override def validateRequest(securable: Securable): Boolean = securable == Catalog
    override def validateGrant(securable: Securable): Boolean = securable == Catalog
  }

  /** Gives a user ability to grant a given action */
  case class Grant(action: Action) extends Action {
    override val name: String = Grant.prefix + action.name
    override def validateRequest(securable: Securable): Boolean = action match {
      case Grant(_) | Own => false
      case _ => action.validateRequest(securable)
    }
    override def validateGrant(securable: Securable): Boolean = action match {
      case Grant(_) | Own => false
      case _ => action.validateGrant(securable)
    }
  }

  object Grant {
    val prefix: String = "GRANT_"
    val prefixLength: Int = prefix.length
  }

  /** All IO actions applicable to files and tables. */
  val allIOActions = Seq(ReadMetadata, Select, Modify)

  /** All catalog related actions. */
  val allCatalogActions: Seq[Action] = allIOActions ++ Seq(Create, CreateNamedFunction)

  /** All non-grant and non-own actions. */
  val allRegularActions: Seq[Action] = allCatalogActions ++ Seq(ModifyClasspath)

  /** All actions except for grant actions. */
  val allActions: Seq[Action] = allRegularActions :+ Own

  /** Convert a regular action into a grant action. */
  def grant(permission: Permission): Permission = {
    require(!permission.action.isInstanceOf[Grant])
    permission.copy(action = Grant(permission.action))
  }

  /** Resolve an [[Action]] using its name. */
  def fromString(name: String): Action = {
    val upperName = name.toUpperCase
    allActions.find(_.name == upperName).getOrElse {
      if (upperName.startsWith(Grant.prefix)) {
        Grant(fromString(upperName.substring(Grant.prefixLength)))
      } else {
        throw new IllegalArgumentException(s"Unknown ActionType $name")
      }
    }
  }
}

/**
 * A Permission request defines actions to be executed on securables. The principal executing the
 * action is implied by the context in which the request is issued; it is either the current user
 * or the owner of the object to which the current user has been granted access (ownership
 * chaining).
 *
 * A permission request can contain the sub-requests it triggers. An example of this is when a user
 * 'x' selects data from view 'y'. The request itself is a [[Action.Select]] on [[Table]] 'y'
 * (there is no difference between reading from views and tables in the security model), and its
 * sub-request are the requests needed to materialize the view.
 */
case class Request(securable: Securable, action: Action, children: Set[Request] = Set.empty) {
  override def toString: String = {
    val builder = new StringBuilder
    treeString(builder, "")
    builder.toString()
  }

  private def treeString(builder: StringBuilder, indent: String): Unit = {
    builder.append(action)
    builder.append(" ")
    builder.append(securable)
    children.foreach { request =>
      builder.append("\n")
      builder.append(indent)
      builder.append(":- ")
      request.treeString(builder, "   ")
    }
  }
}

/**
 * The permission of a principal to execute an action on a securable object.
 */
case class Permission(principal: Principal, action: Action, securable: Securable) {

  def validateRequest: Boolean = action.validateRequest(securable)

  def validateGrant: Boolean = action.validateGrant(securable)
}
