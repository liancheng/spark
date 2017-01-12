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

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

/**
 * An [[Offset]] for the [[KafkaSource]]. This one tracks all partitions of subscribed topics and
 * their offsets.
 *
 * The original file is `org.apache.spark.sql.kafka010.KafkaSourceOffset`.
 */
private[kafka08]
case class KafkaSourceOffset(partitionToOffsets: Map[TopicAndPartition, Long]) extends Offset {

  override val json = JsonUtils.partitionOffsets(partitionToOffsets)
}

/** Companion object of the [[KafkaSourceOffset]] */
private[kafka08] object KafkaSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[TopicAndPartition, Long] = {
    offset match {
      case o: KafkaSourceOffset => o.partitionToOffsets
      case so: SerializedOffset => KafkaSourceOffset(so).partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  /**
   * Returns [[KafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
   * tuples.
   */
  def apply(offsetTuples: (String, Int, Long)*): KafkaSourceOffset = {
    KafkaSourceOffset(offsetTuples.map { case(t, p, o) => (TopicAndPartition(t, p), o) }.toMap)
  }

  /**
   * Returns [[KafkaSourceOffset]] from a JSON [[SerializedOffset]]
   */
  def apply(offset: SerializedOffset): KafkaSourceOffset =
    KafkaSourceOffset(JsonUtils.partitionOffsets(offset.json))
}
