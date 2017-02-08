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

package org.apache.spark.sql.hive.test

import com.databricks.sql.DatabricksStaticSQLConf

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.SparkFunSuite

class TestHiveExtensions(val extensions: SparkSessionExtensions => Unit)
    extends SparkFunSuite {
  private val hiveContext: TestHiveContext = {
    val sc = TestHive.sparkSession.sparkContext
    sc.conf.set(DatabricksStaticSQLConf.ACL_ENABLED.key, "true")
    new TestHiveContext(sc, extensions)
  }

  protected def spark: SparkSession = hiveContext.sparkSession

  protected override def afterAll(): Unit = {
    try {
      hiveContext.reset()
    } finally {
      super.afterAll()
    }
  }
}
