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

package org.apache.spark.sql

import org.apache.hadoop.fs._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

trait CheckedFilesystem extends LocalFileSystem {

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    checkFileAccessAudit(f.toString, "open")
    super.open(f, bufferSize)
  }

  override def listStatus(path: Path): Array[FileStatus] = {
    checkFileAccessAudit(path.toString, "list")
    super.listStatus(path)
  }

  override def exists(path: Path): Boolean = {
    checkFileAccessAudit(path.toString, "exists")
    super.exists(path)
  }

  private def checkFileAccessAudit(file: String, op: String): Unit = {
    val taskContext = TaskContext.get()
    val sparkContext = SparkSession.getActiveSession.map(_.sparkContext).orNull
    if ((sparkContext != null && sparkContext.getLocalProperty("spark.sql.trust.file") !=
        sparkContext.getLocalProperty("spark.sql.trust.id")) ||
        (taskContext != null && taskContext.getLocalProperty("spark.sql.trust.file") !=
          taskContext.getLocalProperty("spark.sql.trust.id"))) {
      throw new RuntimeException(s"Untrusted $op of $file")
    }
  }
}
