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
 * The [[AclProvider]] provides the machinery to create an [[AclClient]]. An [[AclProvider]]
 * must have a public no-arg constructor.
 */
trait AclProvider {
  /**
   * Create the [[AclClient]] for a given [[SparkSession]].
   */
  def create(session: SparkSession): AclClient
}

object AclProvider {
  /**
   * Create an [[AclProvider]] using reflection. The 'spark.databricks.acl.provider' configuration
   * property is used to find the required [[AclProvider]] implementation. An error will be thrown
   * if this property has not been set or when the class cannot be created.
   */
  def provider(session: SparkSession): AclProvider = {
    // Create a provider.
    try {
      val name = session.conf.get(DatabricksStaticSQLConf.ACL_PROVIDER.key)
      // scalastyle:off classforname
      Class.forName(name).newInstance().asInstanceOf[AclProvider]
      // scalastyle:on classforname
    } catch {
      case e @ (_: NoSuchElementException |
                _: ClassNotFoundException |
                _: IllegalAccessException |
                _: ClassCastException) =>
        // This is a show stopper.
        throw new Error(e)
    }
  }

  /**
   * Create an [[AclClient]] for the given [[SparkSession]] using reflection.
   */
  def client(session: SparkSession): AclClient = provider(session).create(session)
}
