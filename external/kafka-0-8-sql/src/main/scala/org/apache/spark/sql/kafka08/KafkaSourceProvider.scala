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

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.kafka08.KafkaSource._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

/**
 * The provider class for the [[KafkaSource]]. This provider is designed such that it throws
 * IllegalArgumentException when the Kafka Dataset is created, so that it can catch
 * missing options even before the query is started.
 *
 * The original file is from `org.apache.spark.sql.kafka010.KafkaSourceProvider`.
 */
private[kafka08] class KafkaSourceProvider extends StreamSourceProvider
  with DataSourceRegister with Logging {

  import KafkaSourceProvider._

  /**
   * Returns the name and schema of the source. In addition, it also verifies whether the options
   * are correct and sufficient to create the [[KafkaSource]] when the query is started.
   */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    validateOptions(parameters)
    (shortName, KafkaSource.kafkaSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    validateOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase.startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val startingOffsets =
      caseInsensitiveParams.get(STARTING_OFFSETS_OPTION_KEY).map(_.trim.toLowerCase) match {
        case Some("latest") => LatestOffsets
        case Some("earliest") => EarliestOffsets
        case Some(json) => SpecificOffsets(JsonUtils.partitionOffsets(json))
        case None => LatestOffsets
      }

    val strategy = caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
      case ("assign", value) =>
        AssignStrategy(JsonUtils.partitions(value).toSet)
      case ("subscribe", value) =>
        SubscribeStrategy(value.split(",").map(_.trim()).filter(_.nonEmpty).toSet)
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    new KafkaSource(
      sqlContext,
      strategy,
      new ju.HashMap(specifiedKafkaParams.asJava),
      parameters,
      metadataPath,
      startingOffsets)
  }

  private def validateOptions(parameters: Map[String, String]): Unit = {

    // Validate source options

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedStrategies =
      caseInsensitiveParams.filter { case (k, _) => STRATEGY_OPTION_KEYS.contains(k) }.toSeq
    if (specifiedStrategies.isEmpty) {
      throw new IllegalArgumentException(
        "One of the following options must be specified for Kafka source: "
          + STRATEGY_OPTION_KEYS.mkString(", ") + ". See the docs for more details.")
    } else if (specifiedStrategies.size > 1) {
      throw new IllegalArgumentException(
        "Only one of the following options can be specified for Kafka source: "
          + STRATEGY_OPTION_KEYS.mkString(", ") + ". See the docs for more details.")
    }

    val strategy = caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
      case ("assign", value) =>
        if (!value.trim.startsWith("{")) {
          throw new IllegalArgumentException(
            "No topicpartitions to assign as specified value for option " +
              s"'assign' is '$value'")
        }

      case ("subscribe", value) =>
        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
        if (topics.isEmpty) {
          throw new IllegalArgumentException(
            "No topics to subscribe to as specified value for option " +
              s"'subscribe' is '$value'")
        }
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    // Validate user-specified Kafka options
    if (!caseInsensitiveParams.contains("kafka.metadata.broker.list") &&
      !caseInsensitiveParams.contains("kafka.bootstrap.servers")) {
      throw new IllegalArgumentException(
        s"Option 'kafka.bootstrap.servers' must be specified for " +
          s"configuring Kafka consumer")
    }
  }

  override def shortName: String = "kafka08"
}

private[kafka08] object KafkaSourceProvider {
  private val STRATEGY_OPTION_KEYS = Set("subscribe", "assign")
  private val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
}
