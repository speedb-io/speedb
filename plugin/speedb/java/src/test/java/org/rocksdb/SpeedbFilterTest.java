// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

public class SpeedbFilterTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();
  @Test
  public void createFromString() throws RocksDBException {
    final BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
    try (final Options options = new Options()) {
      try (final Filter filter = Filter.createFromString("speedb.PairedBloomFilter:20")) {
        assertThat(filter.isInstanceOf("speedb_paired_bloom_filter")).isTrue();
        assertThat(filter.isInstanceOf("speedb.PairedBloomFilter")).isTrue();
        assertThat(filter.isInstanceOf("bloomfilter")).isFalse();
        blockConfig.setFilterPolicy(filter);
        options.setTableFormatConfig(blockConfig);
      }
      try (final Filter filter = Filter.createFromString("speedb_paired_bloom_filter:20")) {
        assertThat(filter.isInstanceOf("speedb_paired_bloom_filter")).isTrue();
        assertThat(filter.isInstanceOf("speedb.PairedBloomFilter")).isTrue();
        assertThat(filter.isInstanceOf("bloomfilter")).isFalse();
        blockConfig.setFilterPolicy(filter);
        options.setTableFormatConfig(blockConfig);
      }
    }
  }
}
