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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSQLContext

class ParquetInteroperabilitySuite extends ParquetCompatibilityTest with SharedSQLContext {
  test("parquet files with different physical schemas but share the same logical schema") {
    import ParquetCompatibilityTest._

    // This test case writes two Parquet files, both representing the following Catalyst schema
    //
    //   StructType(
    //     StructField(
    //       "f",
    //       ArrayType(IntegerType, containsNull = false),
    //       nullable = false))
    //
    // The first Parquet file comes with parquet-avro style 2-level LIST-annotated group, while the
    // other one comes with parquet-protobuf style 1-level unannotated primitive field.
    withTempDir { dir =>
      val avroStylePath = new File(dir, "avro-style").getCanonicalPath
      val protobufStylePath = new File(dir, "protobuf-style").getCanonicalPath

      val avroStyleSchema =
        """message avro_style {
          |  required group f (LIST) {
          |    repeated int32 array;
          |  }
          |}
        """.stripMargin

      writeDirect(avroStylePath, avroStyleSchema, { rc =>
        rc.message {
          rc.field("f", 0) {
            rc.group {
              rc.field("array", 0) {
                rc.addInteger(0)
                rc.addInteger(1)
              }
            }
          }
        }
      })

      logParquetSchema(avroStylePath)

      val protobufStyleSchema =
        """message protobuf_style {
          |  repeated int32 f;
          |}
        """.stripMargin

      writeDirect(protobufStylePath, protobufStyleSchema, { rc =>
        rc.message {
          rc.field("f", 0) {
            rc.addInteger(2)
            rc.addInteger(3)
          }
        }
      })

      logParquetSchema(protobufStylePath)

      checkAnswer(
        sqlContext.read.parquet(dir.getCanonicalPath),
        Seq(
          Row(Seq(0, 1)),
          Row(Seq(2, 3))))
    }
  }

  test("foo") {
    import ParquetCompatibilityTest._

    withTempDir { dir =>
      val originalPath = new File(dir, "original").getCanonicalPath

      val originalParquetSchema =
        """message root {
          |  required group f0 (LIST) {
          |    repeated group array {
          |      required int64 element (INT_64);
          |    }
          |  }
          |}
        """.stripMargin

      writeDirect(originalPath, originalParquetSchema, { rc =>
        rc.message {
          rc.field("f0", 0) {
            rc.group {
              rc.field("array", 0) {
                rc.group {
                  rc.field("element", 0) {
                    rc.addLong(42)
                  }
                }
              }
            }
          }
        }
      })

      val df = sqlContext.read.parquet(originalPath)

      df.printSchema()
      // root
      // |-- f0: array (nullable = false)
      // |    |-- element: struct (containsNull = false)
      // |    |    |-- element: long (nullable = false)

      df.show()
      // +------+
      // |    f0|
      // +------+
      // |[[42]]|
      // +------+

      checkAnswer(df, Row(Array(Row(42))))

      val sparkPath = new File(dir, "spark").getCanonicalPath
      df.write.parquet(sparkPath)

      val readBack = sqlContext.read.parquet(sparkPath)

      readBack.printSchema()
      // root
      // |-- f0: array (nullable = false)
      // |    |-- element: struct (containsNull = false)
      // |    |    |-- element: long (nullable = false)

      readBack.show()
      // Crash
    }
  }
}
