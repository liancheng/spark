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

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import kafka.common.TopicAndPartition
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * Utilities for converting Kafka related objects to and from json.
 *
 * The original file is `org.apache.spark.sql.kafka010.JsonUtils`.
 */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * Read TopicPartitions from json string
   */
  def partitions(str: String): Array[TopicAndPartition] = {
    try {
      Serialization.read[Map[String, Seq[Int]]](str).flatMap {  case (topic, parts) =>
          parts.map { part =>
            new TopicAndPartition(topic, part)
          }
      }.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":[0,1],"topicB":[0,1]}, got $str""")
    }
  }

  /**
   * Write TopicPartitions as json string
   */
  def partitions(partitions: Iterable[TopicAndPartition]): String = {
    val result = new HashMap[String, List[Int]]
    partitions.foreach { tp =>
      val parts: List[Int] = result.getOrElse(tp.topic, Nil)
      result += tp.topic -> (tp.partition::parts)
    }
    Serialization.write(result)
  }

  /**
   * Read per-TopicPartition offsets from json string
   */
  def partitionOffsets(str: String): Map[TopicAndPartition, Long] = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
          partOffsets.map { case (part, offset) =>
              TopicAndPartition(topic, part) -> offset
          }
      }
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  /**
   * Write per-TopicPartition offsets as json string
   */
  def partitionOffsets(partitionOffsets: Map[TopicAndPartition, Long]): String = {
    val result = new HashMap[String, HashMap[Int, Long]]()
    implicit val ordering = new Ordering[TopicAndPartition] {
      override def compare(x: TopicAndPartition, y: TopicAndPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    val partitions = partitionOffsets.keySet.toSeq.sorted  // sort for more determinism
    partitions.foreach { tp =>
        val off = partitionOffsets(tp)
        val parts = result.getOrElse(tp.topic, new HashMap[Int, Long])
        parts += tp.partition -> off
        result += tp.topic -> parts
    }
    Serialization.write(result)
  }
}
