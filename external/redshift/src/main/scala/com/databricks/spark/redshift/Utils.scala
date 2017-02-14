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
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory

/**
 * Various arbitrary helper functions
 */
private[redshift] object Utils {

  private val log = LoggerFactory.getLogger(getClass)

  def classForName(className: String): Class[_] = {
    val classLoader =
      Option(Thread.currentThread().getContextClassLoader).getOrElse(this.getClass.getClassLoader)
    // scalastyle:off
    Class.forName(className, true, classLoader)
    // scalastyle:on
  }

  /**
   * Joins prefix URL a to path suffix b, and appends a trailing /, in order to create
   * a temp directory path for S3.
   */
  def joinUrls(a: String, b: String): String = {
    a.stripSuffix("/") + "/" + b.stripPrefix("/").stripSuffix("/") + "/"
  }

  /**
   * Redshift COPY and UNLOAD commands don't support s3n or s3a, but users may wish to use them
   * for data loads. This function converts the URL back to the s3:// format.
   */
  def fixS3Url(url: String): String = {
    url.replaceAll("s3[an]://", "s3://")
  }

  /**
   * Factory method to create new S3URI in order to handle various library incompatibilities with
   * older AWS Java Libraries
   */
  def createS3URI(url: String): AmazonS3URI = {
    try {
      // try to instantiate AmazonS3URI with url
      new AmazonS3URI(url)
    } catch {
      case e: IllegalArgumentException if e.getMessage.
        startsWith("Invalid S3 URI: hostname does not appear to be a valid S3 endpoint") => {
        new AmazonS3URI(addEndpointToUrl(url))
      }
    }
  }

  /**
   * Since older AWS Java Libraries do not handle S3 urls that have just the bucket name
   * as the host, add the endpoint to the host
   */
  def addEndpointToUrl(url: String, domain: String = "s3.amazonaws.com"): String = {
    val uri = new URI(url)
    val hostWithEndpoint = uri.getHost + "." + domain
    new URI(uri.getScheme,
      uri.getUserInfo,
      hostWithEndpoint,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment).toString
  }

  /**
   * Returns a copy of the given URI with the user credentials removed.
   */
  def removeCredentialsFromURI(uri: URI): URI = {
    new URI(
      uri.getScheme,
      null, // no user info
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment)
  }

  // Visible for testing
  private[redshift] var lastTempPathGenerated: String = null

  /**
   * Creates a randomly named temp directory path for intermediate data
   */
  def makeTempPath(tempRoot: String): String = {
    lastTempPathGenerated = Utils.joinUrls(tempRoot, UUID.randomUUID().toString)
    lastTempPathGenerated
  }

  /**
   * Checks whether the S3 bucket for the given UI has an object lifecycle configuration to
   * ensure cleanup of temporary files. If no applicable configuration is found, this method logs
   * a helpful warning for the user.
   */
  def checkThatBucketHasObjectLifecycleConfiguration(
      tempDir: String,
      s3Client: AmazonS3Client): Unit = {
    try {
      val s3URI = createS3URI(Utils.fixS3Url(tempDir))
      val bucket = s3URI.getBucket
      assert(bucket != null, "Could not get bucket from S3 URI")
      val key = Option(s3URI.getKey).getOrElse("")
      val hasMatchingBucketLifecycleRule: Boolean = {
        val rules = Option(s3Client.getBucketLifecycleConfiguration(bucket))
          .map(_.getRules.asScala)
          .getOrElse(Seq.empty)
        rules.exists { rule =>
          // Note: this only checks that there is an active rule which matches the temp directory;
          // it does not actually check that the rule will delete the files. This check is still
          // better than nothing, though, and we can always improve it later.
          rule.getStatus == BucketLifecycleConfiguration.ENABLED && key.startsWith(rule.getPrefix)
        }
      }
      if (!hasMatchingBucketLifecycleRule) {
        log.warn(s"The S3 bucket $bucket does not have an object lifecycle configuration to " +
          "ensure cleanup of temporary files. Consider configuring `tempdir` to point to a " +
          "bucket with an object lifecycle policy that automatically deletes files after an " +
          "expiration period. For more information, see " +
          "https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html")
      }
    } catch {
      case NonFatal(e) =>
        log.warn("An error occurred while trying to read the S3 bucket lifecycle configuration", e)
    }
  }

  /**
   * Given a URI, verify that the Hadoop FileSystem for that URI is not the S3 block FileSystem.
   * `spark-redshift` cannot use this FileSystem because the files written to it will not be
   * readable by Redshift (and vice versa).
   */
  def assertThatFileSystemIsNotS3BlockFileSystem(uri: URI, hadoopConfig: Configuration): Unit = {
    val fs = FileSystem.get(uri, hadoopConfig)
    // Note that we do not want to use isInstanceOf here, since we're only interested in detecting
    // exact matches. We compare the class names as strings in order to avoid introducing a binary
    // dependency on classes which belong to the `hadoop-aws` JAR, as that artifact is not present
    // in some environments (such as EMR). See #92 for details.
    if (fs.getClass.getCanonicalName == "org.apache.hadoop.fs.s3.S3FileSystem") {
      throw new IllegalArgumentException(
        "spark-redshift does not support the S3 Block FileSystem. Please reconfigure `tempdir` to" +
        "use a s3n:// or s3a:// scheme.")
    }
  }

  /**
   * Attempts to retrieve the region of the S3 bucket.
   */
  def getRegionForS3Bucket(tempDir: String, s3Client: AmazonS3Client): Option[String] = {
    try {
      val s3URI = createS3URI(Utils.fixS3Url(tempDir))
      val bucket = s3URI.getBucket
      assert(bucket != null, "Could not get bucket from S3 URI")
      val region = s3Client.getBucketLocation(bucket) match {
        // Map "US Standard" to us-east-1
        case null | "US" => "us-east-1"
        case other => other
      }
      Some(region)
    } catch {
      case NonFatal(e) =>
        log.warn("An error occurred while trying to determine the S3 bucket's region", e)
        None
    }
  }

  /**
   * Attempts to determine the region of a Redshift cluster based on its URL. It may not be possible
   * to determine the region in some cases, such as when the Redshift cluster is placed behind a
   * proxy.
   */
  def getRegionForRedshiftCluster(url: String): Option[String] = {
    val regionRegex = """.*\.([^.]+)\.redshift\.amazonaws\.com.*""".r
    url match {
      case regionRegex(region) => Some(region)
      case _ => None
    }
  }

  /**
   * Escapes a string, so that it can be passed as a JDBC string literal.
   */
  def escapeJdbcString(s: String): String = {
    s.replace("\\", "\\\\").replace("'", "\\'")
  }

  /**
   * Escapes a string, so that it can be passed as literal within a Redshift SQL query.
   */
  def escapeRedshiftStringLiteral(s: String): String = {
    // Note that one may expect here ' -> '' escaping be enough for string literals, but apparently
    // backslashes also need to be escaped; otherwise a string literal ending in \ will cause
    // Redshift to complain that the string is unterminated because it interprets the \ at the end
    // as an escape character for the closing single-quote that follows it.
    s.replace("\\", "\\\\").replace("'", "''")
  }

  /**
   * Compares two version strings, returning <0 for "lower", 0 for "equal" and >0 for "higher".
   *
   * Extracts all numbers from the given input strings and throws exception if the two resulting
   * lists are of different sizes.
   */
  def compareVersions(str1: String, str2: String): Int = {
    val numberPattern = "[0-9]+".r

    val vals1 = numberPattern.findAllIn(str1).map(Integer.valueOf)
    val vals2 = numberPattern.findAllIn(str2).map(Integer.valueOf)

    if (vals1.length != vals2.length) {
      throw new IllegalArgumentException(s"Versions ${str1} and ${str2} have different sizes.")
    }

    (vals1 zip vals2) collectFirst { case (v1, v2) if v1 != v2 => v1 - v2} getOrElse 0
  }
}
