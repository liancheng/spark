/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.hadoop.fs.s3native;

import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.fs.s3native.InMemoryNativeFileSystemStore;

/**
 * A helper implementation of {@link NativeS3FileSystem}
 * without actually connecting to S3 for unit testing.
 */
public class S3NInMemoryFileSystem extends NativeS3FileSystem {
  public S3NInMemoryFileSystem() {
    super(new InMemoryNativeFileSystemStore());
  }
}
