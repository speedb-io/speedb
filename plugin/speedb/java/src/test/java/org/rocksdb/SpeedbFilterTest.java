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
