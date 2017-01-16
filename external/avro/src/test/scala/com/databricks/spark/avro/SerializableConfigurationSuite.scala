/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.avro

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}

class SerializableConfigurationSuite extends SparkFunSuite {

  private def testSerialization(serializer: SerializerInstance): Unit = {
    import DefaultSource.SerializableConfiguration
    val conf = new SerializableConfiguration(new Configuration())

    val serialized = serializer.serialize(conf)

    serializer.deserialize[Any](serialized) match {
      case c: SerializableConfiguration =>
        assert(c.log != null, "log was null")
        assert(c.value != null, "value was null")
      case other => fail(
        s"Expecting ${classOf[SerializableConfiguration]}, but got ${other.getClass}.")
    }
  }

  test("serialization with JavaSerializer") {
    testSerialization(new JavaSerializer(new SparkConf()).newInstance())
  }

  test("serialization with KryoSerializer") {
    testSerialization(new KryoSerializer(new SparkConf()).newInstance())
  }

}
