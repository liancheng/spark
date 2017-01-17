/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import java.net.URI

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for helper functions
 */
class UtilsSuite extends SparkFunSuite with Matchers {

  test("joinUrls preserves protocol information") {
    Utils.joinUrls("s3n://foo/bar/", "/baz") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "/baz/") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "baz/") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "baz") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar", "baz") shouldBe "s3n://foo/bar/baz/"
  }

  test("joinUrls preserves credentials") {
    assert(
      Utils.joinUrls("s3n://ACCESSKEY:SECRETKEY@bucket/tempdir", "subdir") ===
      "s3n://ACCESSKEY:SECRETKEY@bucket/tempdir/subdir/")
  }

  test("fixUrl produces Redshift-compatible equivalents") {
    Utils.fixS3Url("s3a://foo/bar/12345") shouldBe "s3://foo/bar/12345"
    Utils.fixS3Url("s3n://foo/bar/baz") shouldBe "s3://foo/bar/baz"
  }

  test("addEndpointToUrl produces urls with endpoints added to host") {
    Utils.addEndpointToUrl("s3a://foo/bar/12345") shouldBe "s3a://foo.s3.amazonaws.com/bar/12345"
    Utils.addEndpointToUrl("s3n://foo/bar/baz") shouldBe "s3n://foo.s3.amazonaws.com/bar/baz"
  }

  test("temp paths are random subdirectories of root") {
    val root = "s3n://temp/"
    val firstTempPath = Utils.makeTempPath(root)

    Utils.makeTempPath(root) should (startWith (root) and endWith ("/")
      and not equal root and not equal firstTempPath)
  }

  test("removeCredentialsFromURI removes AWS access keys") {
    def removeCreds(uri: String): String = {
      Utils.removeCredentialsFromURI(URI.create(uri)).toString
    }
    assert(removeCreds("s3n://bucket/path/to/temp/dir") === "s3n://bucket/path/to/temp/dir")
    assert(
      removeCreds("s3n://ACCESSKEY:SECRETKEY@bucket/path/to/temp/dir") ===
      "s3n://bucket/path/to/temp/dir")
  }

  test("getRegionForRedshiftCluster") {
    val redshiftUrl =
      "jdbc:redshift://example.secret.us-west-2.redshift.amazonaws.com:5439/database"
    assert(Utils.getRegionForRedshiftCluster("mycluster.example.com") === None)
    assert(Utils.getRegionForRedshiftCluster(redshiftUrl) === Some("us-west-2"))
  }
}
