/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import java.io._

import scala.util.control.NonFatal

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

class SerializableConfiguration(@transient var value: Configuration)
    extends Serializable with KryoSerializable {
  @transient private[redshift] lazy val log = LoggerFactory.getLogger(getClass)

  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }

  private def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        log.error("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        log.error("Exception encountered", e)
        throw new IOException(e)
    }
  }

  def write(kryo: Kryo, out: Output): Unit = {
    val dos = new DataOutputStream(out)
    value.write(dos)
    dos.flush()
  }

  def read(kryo: Kryo, in: Input): Unit = {
    value = new Configuration(false)
    value.readFields(new DataInputStream(in))
  }
}
