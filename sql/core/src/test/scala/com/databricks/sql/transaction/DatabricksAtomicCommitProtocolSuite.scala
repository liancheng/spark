/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.sql.transaction

import java.io._

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.{ManualClock, SystemClock}

class DatabricksAtomicCommitProtocolSuite extends QueryTest with SharedSQLContext {
  test("read protocol ignores uncommitted jobs") {
    withTempDir { dir =>
      create(dir, "_started_12345")
      create(dir, "part-r-00001-tid-77777-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv")
      create(dir, "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv")
      assert(spark.read.csv(dir.getAbsolutePath).inputFiles.length == 1)
    }
  }

  test("read protocol can be flag disabled") {
    withTempDir { dir =>
      create(dir, "_started_12345")
      create(dir, "part-r-00001-tid-77777-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv")
      create(dir, "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv")
      try {
        SparkEnv.get.conf.set("spark.databricks.sql.enableFilterUncommitted", "false")
        assert(spark.read.csv(dir.getAbsolutePath).count == 2)
        assert(spark.read.csv(dir.getAbsolutePath).inputFiles.length == 2)
      } finally {
        SparkEnv.get.conf.remove("spark.databricks.sql.enableFilterUncommitted")
      }
    }
  }

  test("read protocol ignores uncommitted files of committed job") {
    withTempDir { dir =>
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f2 = "part-r-00002-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f3 = "part-r-00003-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)
      create(dir, f2)
      create(dir, f3)
      assert(spark.read.csv(dir.getAbsolutePath).inputFiles.length == 3)
      create(dir, "_committed_12345", s"""{"added": ["$f1", "$f2"], "removed": []}""")
      assert(spark.read.csv(dir.getAbsolutePath).inputFiles.length == 2)
      create(dir, "_started_12345")  // shouldn't matter if _started marker exists
      assert(spark.read.csv(dir.getAbsolutePath).inputFiles.length == 2)
    }
  }

  test("read protocol ignores committed deletes") {
    withTempDir { dir =>
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f2 = "part-r-00002-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f3 = "part-r-00003-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f4 = "foo"
      create(dir, f1)
      create(dir, f2)
      create(dir, f3)
      create(dir, f4)
      assert(spark.read.csv(dir.getAbsolutePath).inputFiles.length == 4)
      create(dir, "_committed_12345", s"""{"added": ["$f1", "$f2", "$f3"], "removed": []}""")
      assert(spark.read.csv(dir.getAbsolutePath).inputFiles.length == 4)
      create(dir, "_committed_99999", s"""{"added": ["foo"], "removed": ["$f3", "$f4"]}""")
      assert(spark.read.csv(dir.getAbsolutePath).inputFiles.length == 2)
    }
  }

  test("vacuum removes files from failed tasks immediately") {
    withTempDir { dir =>
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f2 = "part-r-00002-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)
      create(dir, f2)
      create(dir, "_committed_12345", s"""{"added": ["$f2"], "removed": []}""")
      assert(list(dir) == Set(f1, f2, "_committed_12345"))
      // removes f1
      assert(sql(s"VACUUM '${dir.getAbsolutePath}'").count == 1)
      assert(list(dir) == Set(f2, "_committed_12345"))
      // removes nothing
      assert(sql(s"VACUUM '${dir.getAbsolutePath}' RETAIN 0.0 HOURS").count == 0)
      assert(list(dir) == Set(f2, "_committed_12345"))
    }
  }

  test("vacuum removes uncommitted files after timeout") {
    withTempDir { dir =>
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f2 = "part-r-00002-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f3 = "part-r-00003-tid-9999-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)
      create(dir, f2)
      create(dir, f3)
      create(dir, "_started_9999")
      create(dir, "_started_55555")
      create(dir, "_committed_55555", s"""{"added": [], "removed": []}""")
      create(dir, "_committed_12345", s"""{"added": ["$f2"], "removed": []}""")
      assert(list(dir).size == 7)
      // removes f1, since this is immediately useless
      assert(sql(s"VACUUM '${dir.getAbsolutePath}'").count == 1)
      assert(list(dir) ==
        Set(f2, f3, "_started_9999", "_started_55555", "_committed_55555", "_committed_12345"))
      // removes f3, unnecessary start marker after horizon
      assert(sql(s"VACUUM '${dir.getAbsolutePath}' RETAIN 0.0 HOURS").count == 2)
      assert(list(dir) ==
        Set(f2, "_started_9999", "_committed_55555", "_committed_12345"))
    }
  }

  test("vacuum removes deleted files after timeout") {
    withTempDir { dir =>
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f2 = "part-r-00002-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      val f3 = "part-r-00003-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)
      create(dir, f2)
      create(dir, f3)
      create(dir, "_committed_99999", s"""{"added": ["foo"], "removed": ["$f3"]}""")
      assert(list(dir).size == 4)
      assert(sql(s"VACUUM '${dir.getAbsolutePath}'").count == 0)
      assert(list(dir).size == 4)
      // removes f3
      assert(sql(s"VACUUM '${dir.getAbsolutePath}' RETAIN 0.0 HOURS").count == 1)
      assert(list(dir) == Set(f1, f2, "_committed_99999"))
    }
  }

  test("zero-length commit markers are ignored") {
    withTempDir { dir =>
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)
      create(dir, "_committed_12345", "")
      assert(spark.read.csv(dir.getAbsolutePath).count == 1)
    }
  }

  test("zero-length commit markers mean txn is pending") {
    withTempDir { dir =>
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)
      create(dir, "_started_12345")
      create(dir, "_committed_12345", "")
      // nothing removed with normal horizon since the txn counts as pending
      assert(sql(s"VACUUM '${dir.getAbsolutePath}'").count == 0)
      // removes f1
      assert(sql(s"VACUUM '${dir.getAbsolutePath}' RETAIN 0.0 HOURS").count == 1)
    }
  }

  test("corrupt commit markers raises error unless configured not to") {
    withTempDir { dir =>
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)
      create(dir, "_committed_12345", "corrupt_file")
      val error = intercept[IOException] {
        spark.read.csv(dir.getAbsolutePath)
      }
      assert(error.getMessage.contains("Failed to read job commit marker"))
      try {
        SparkEnv.get.conf.set("spark.databricks.sql.ignoreCorruptCommitMarkers", "true")
        assert(spark.read.csv(dir.getAbsolutePath).count == 1)
      } finally {
        SparkEnv.get.conf.remove("spark.databricks.sql.ignoreCorruptCommitMarkers")
      }
    }
  }

  test("reader re-lists directory when files may be missing from the initial list") {
    withTempDir { dir =>
      var listCount = 0
      val fs = new RawLocalFileSystem() {
        override def listStatus(path: Path): Array[FileStatus] = {
          listCount += 1
          super.listStatus(path)
        }
      }
      fs.initialize(dir.toURI, new Configuration())
      val testPath = new Path(dir.getAbsolutePath)
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)

      // should trigger a re-list since f1 had no associated marker
      val in1 = fs.listStatus(testPath)
      listCount = 0
      val (out1, _) = DatabricksAtomicReadProtocol.resolveCommitState(fs, testPath, in1)
      assert(listCount == 1)
      assert(out1.isCommitted("12345"))  // couldn't find any marker, assumed committed

      // should trigger a re-list that picks up _started_12345
      create(dir, "_started_12345")
      listCount = 0
      val in2 = in1
      val (out2, _) = DatabricksAtomicReadProtocol.resolveCommitState(fs, testPath, in2)
      assert(listCount == 1)
      assert(!out2.isCommitted("12345"))  // marker found on the second list

      // should NOT trigger a re-list since f1 had an associated marker
      val in3 = fs.listStatus(testPath)
      listCount = 0
      val (out3, _) = DatabricksAtomicReadProtocol.resolveCommitState(fs, testPath, in3)
      assert(listCount == 0)
      assert(!out3.isCommitted("12345"))

      // also should not trigger a re-list
      fs.delete(new Path(testPath, "_started_12345"), false)
      create(dir, "_committed_12345", s"""{"added": [], "removed": []}""")
      val in4 = fs.listStatus(testPath)
      listCount = 0
      val (out4, _) = DatabricksAtomicReadProtocol.resolveCommitState(fs, testPath, in4)
      assert(listCount == 0)
      assert(out4.isCommitted("12345"))

      // should trigger a re-list that picks up f2
      val f2 = "part-r-00002-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, "_committed_12345", s"""{"added": ["$f2"], "removed": []}""")
      val in5 = fs.listStatus(testPath)
      create(dir, f2)
      listCount = 0
      val (out5, _) = DatabricksAtomicReadProtocol.resolveCommitState(fs, testPath, in5)
      assert(listCount == 1)
      assert(out5.isCommitted("12345"))
      assert(out5.isFileCommitted("12345", f2))
    }
  }

  test("re-list is avoided after a grace period") {
    withTempDir { dir =>
      var listCount = 0
      val fs = new RawLocalFileSystem() {
        override def listStatus(path: Path): Array[FileStatus] = {
          listCount += 1
          super.listStatus(path)
        }
      }
      fs.initialize(dir.toURI, new Configuration())
      val testPath = new Path(dir.getAbsolutePath)
      val f1 = "part-r-00001-tid-12345-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-0_00003.csv"
      create(dir, f1)

      try {
        val clock = new ManualClock(System.currentTimeMillis)
        DatabricksAtomicReadProtocol.clock = clock

        // should trigger a re-list since f1 had no associated marker
        val in1 = fs.listStatus(testPath)
        listCount = 0
        DatabricksAtomicReadProtocol.resolveCommitState(fs, testPath, in1)
        assert(listCount == 1)

        clock.advance(3 * 60 * 1000)
        DatabricksAtomicReadProtocol.resolveCommitState(fs, testPath, in1)
        assert(listCount == 2)

        clock.advance(3 * 60 * 1000)
        DatabricksAtomicReadProtocol.resolveCommitState(fs, testPath, in1)
        assert(listCount == 2)
      } finally {
        DatabricksAtomicReadProtocol.clock = new SystemClock
      }
    }
  }

  test("randomized consistency stress test") {
    val seed = System.currentTimeMillis
    val random = new scala.util.Random(seed)
    // scalastyle:off println
    println("Random seed used was: " + seed)
    // scalastyle:on println

    /**
     * Emulates S3 list consistency guarantees. We assume read-after-write for single keys,
     * however a list call is not atomic and so may observe writes out of order.
     */
    var numLists = 0
    val inconsistentFs = new RawLocalFileSystem() {
      val consistentFiles = mutable.Set[Path]()
      override def listStatus(path: Path): Array[FileStatus] = {
        numLists += 1
        super.listStatus(path).filter { stat =>
          stat.getPath match {
            case path if consistentFiles.contains(path) => true
            case path =>
              consistentFiles.add(path)
              random.nextDouble > 0.5  // emulate write re-ordering
          }
        }
      }
    }
    def countFiles(dir: File): Long = {
      val idx = new InMemoryFileIndex(spark, Seq(new Path(dir.getAbsolutePath)), Map.empty, None)
      idx.allFiles().length
    }
    inconsistentFs.initialize(new File("/").toURI, new Configuration())

    // tests retry on missing data file or start marker
    for (i <- 1 to 10) {
      withTempDir { dir =>
        spark.range(10).repartition(3).write.mode("overwrite").parquet(dir.getAbsolutePath)
        try {
          DatabricksAtomicReadProtocol.testingFs = Some(inconsistentFs)
          assert(Set(0L, 3L).contains(countFiles(dir)))  // should never see {1, 2}
          assert(countFiles(dir) == 3)
        } finally {
          DatabricksAtomicReadProtocol.testingFs = None
        }
      }
    }

    // check we actually used the inconsistent test fs
    assert(numLists >= 10)
  }

  private def create(dir: File, name: String, contents: String = "foo"): Unit = {
    val printWriter = new PrintWriter(new File(dir, name))
    try {
      printWriter.print(contents)
    } finally {
      printWriter.close()
    }
  }

  private def list(dir: File): Set[String] = {
    dir.listFiles().map(_.getName).filterNot(_.startsWith(".")).toSet
  }
}
