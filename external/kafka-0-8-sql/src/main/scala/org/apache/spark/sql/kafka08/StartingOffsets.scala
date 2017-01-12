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
/*
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.kafka08

import kafka.common.TopicAndPartition

/**
 * Values that can be specified for config startingOffsets.
 *
 * The original file is ` org.apache.spark.sql.kafka010.StartingOffsets`.
 */
private[kafka08] sealed trait StartingOffsets

private[kafka08] case object EarliestOffsets extends StartingOffsets

private[kafka08] case object LatestOffsets extends StartingOffsets

private[kafka08] case class SpecificOffsets(
  partitionOffsets: Map[TopicAndPartition, Long]) extends StartingOffsets
