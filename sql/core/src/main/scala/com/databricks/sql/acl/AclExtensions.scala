/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import scala.util.control.NonFatal

import com.databricks.sql.DatabricksStaticSQLConf
import com.databricks.sql.parser.DatabricksSqlParser

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalog.BaseCatalogHooks
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Databricks Spark ACL configuration object. In order to use this, you will need to pass the
 * following configurations when constructing the SparkSession:
 * 1. spark.sql.extensions = "com.databricks.sql.acl.AclExtensions"
 * 2. spark.databricks.acl.provider = "[Name of an AclProvider]"
 * 3. spark.databricks.acl.client = "[Name of the DB side ACL client]". This is only needed when
 *    we use the [[ReflectionBackedAclProvider]] as the provider.
 */
class AclExtensions extends (SparkSessionExtensions => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectAnalyzerRule { session =>
      if (isAclEnabled(session)) {
        AddExplainNode
      } else {
        NoOpRule
      }
    }
    extensions.injectAnalyzerRule { session =>
      if (isAclEnabled(session)) {
        new ResolveDropCommand(session)
      } else {
        NoOpRule
      }
    }
    extensions.injectCheckAnalysisRule { session =>
      if (isAclEnabled(session)) {
        createPermissionsCheck(session)
      } else {
        _ => ()
      }
    }
    extensions.injectOptimizerRule { session =>
      if (isAclEnabled(session)) {
        CleanUpExplainNode
      } else {
        NoOpRule
      }
    }
    extensions.injectParser { (session, delegate) =>
      if (isAclEnabled(session)) {
        new DatabricksSqlParser(Some(client(session)), delegate)
      } else {
        delegate
      }
    }
    extensions.injectCatalogHooks { session =>
      if (isAclEnabled(session)) {
        new AclIntegratedCatalogHooks(client(session))
      } else {
        new BaseCatalogHooks
      }
    }
  }

  /**
   * Check if ACLs are enabled.
   */
  private def isAclEnabled(session: SparkSession): Boolean = {
    try {
      session.conf.get(DatabricksStaticSQLConf.ACL_ENABLED.key, "false").toBoolean
    } catch {
      case NonFatal(_) => false
    }
  }

  /**
   * Create an [[AclClient]]. This is only visible for testing.
   */
  protected def client(session: SparkSession): AclClient = AclProvider.client(session)

  /**
   * Create a permission checker. This rule is dependent on the use of sql-core or hive, and needs
   * to be modifiable.
   */
  protected def createPermissionsCheck(session: SparkSession): LogicalPlan => Unit = {
    new CheckPermissions(session.catalog, client(session))
  }
}

/**
 * No-op rule used when ACLs are disabled.
 */
object NoOpRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}
