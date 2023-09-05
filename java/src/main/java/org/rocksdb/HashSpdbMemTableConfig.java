// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

/**
 * The config for hash spdbd memtable representation.
 */
public class HashSpdbMemTableConfig extends MemTableConfig {
  public static final int DEFAULT_BUCKET_COUNT = 1000000;

  /**
   * HashSpdbMemTableConfig constructor
   */
  public HashSpdbMemTableConfig() {
    bucketCount_ = DEFAULT_BUCKET_COUNT;
  }

  /**
   * Set the number of hash buckets used in the hash spdb memtable.
   * Default = 1000000.
   *
   * @param count the number of hash buckets used in the hash
   *    spdb memtable.
   * @return the reference to the current HashSpdbMemTableConfig.
   */
  public HashSpdbMemTableConfig setBucketCount(final long count) {
    bucketCount_ = count;
    return this;
  }

  /**
   * @return the number of hash buckets
   */
  public long bucketCount() {
    return bucketCount_;
  }

  @Override
  protected long newMemTableFactoryHandle() {
    return newMemTableFactoryHandle(bucketCount_);
  }

  private native long newMemTableFactoryHandle(long bucketCount) throws IllegalArgumentException;

  private long bucketCount_;
}
