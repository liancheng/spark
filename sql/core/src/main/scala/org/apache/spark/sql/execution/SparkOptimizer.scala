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

package org.apache.spark.sql.execution

import org.apache.spark.sql.ExperimentalMethods
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.closure.TranslateClosureOptimizerRule
import org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions
import org.apache.spark.sql.execution.python.ExtractPythonUDFFromAggregate
import org.apache.spark.sql.internal.SQLConf

class SparkOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods,
    extraOptimizationRules: Seq[Rule[LogicalPlan]])
  extends Optimizer(catalog, conf) {

  override def batches: Seq[Batch] = {
    val finishAnalysis = super.batches.head
    val defaultOptimizers = super.batches.tail
    Seq(
      finishAnalysis,
      // Tries to translate the typed plan like TypedFilter to untyped plan, by translating
      // Java closure to Catalyst expressions
      Batch("Translate Closure", Once, new TranslateClosureOptimizerRule(conf))) ++
      defaultOptimizers :+
      Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog, conf)) :+
      Batch("Extract Python UDF from Aggregate", Once, ExtractPythonUDFFromAggregate) :+
      Batch("Prune File Source Table Partitions", Once, PruneFileSourcePartitions) :+
      Batch("User Provided Optimizers", fixedPoint,
        experimentalMethods.extraOptimizations ++ extraOptimizationRules: _*)
  }
}
