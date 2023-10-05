// Copyright (C) 2023 Speedb Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
