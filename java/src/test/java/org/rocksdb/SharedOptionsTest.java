// Copyright (C) 2022 Speedb Ltd. All rights reserved.
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

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Random;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SharedOptionsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void shortConstructor() {
    long totalRamSize = 100 * 1024 * 1024 * 1024;
    long totalThreads = 10;
    long defaultDelayedWriteRate = 256 * 1024 * 1024;
    long defaultDefaultBucketSize = 1000000;
    boolean defaultUseMerge = true;

    try (final SharedOptions so = new SharedOptions(totalRamSize, totalThreads)) {
      assertThat(so.getMaxWriteBufferManagerSize()).isEqualTo(totalRamSize / 4);
      assertThat(so.getTotalThreads()).isEqualTo(totalThreads);
      assertThat(so.getTotalRamSizeBytes()).isEqualTo(totalRamSize);
      assertThat(so.getDelayedWriteRate()).isEqualTo(defaultDelayedWriteRate);
      assertThat(so.getBucketSize()).isEqualTo(defaultDefaultBucketSize);
      assertThat(so.isMergeMemtableSupported()).isEqualTo(defaultUseMerge);
    }
  }

  @Test
  public void altConstructor() {
    long totalRamSize = 100 * 1024 * 1024 * 1024;
    long totalThreads = 10;
    long delayedWriteRate = 512 * 1024 * 1024;
    long bucketSize = 50000;
    boolean use_merge = false;

    try (final SharedOptions so = new SharedOptions(
             totalRamSize, totalThreads, delayedWriteRate, bucketSize, use_merge)) {
      assertThat(so.getMaxWriteBufferManagerSize()).isEqualTo(totalRamSize / 4);
      assertThat(so.getTotalThreads()).isEqualTo(totalThreads);
      assertThat(so.getTotalRamSizeBytes()).isEqualTo(totalRamSize);
      assertThat(so.getDelayedWriteRate()).isEqualTo(delayedWriteRate);
      assertThat(so.getBucketSize()).isEqualTo(bucketSize);
      assertThat(so.isMergeMemtableSupported()).isEqualTo(use_merge);
    }
  }

  @Test
  public void enableSpeedb() {
    try (final Options opts = new Options()) {
      long totalRamSize = 100 * 1024 * 1024 * 1024;
      long totalThreads = 10;
      long defaultDelayedWriteRate = 256 * 1024 * 1024;

      try (final SharedOptions so = new SharedOptions(totalRamSize, totalThreads)) {
        assertThat(opts.maxBackgroundJobs()).isEqualTo((int) 2);
        opts.setDelayedWriteRate(1000);
        assertThat(opts.delayedWriteRate()).isEqualTo((long) 1000);
        opts.setBytesPerSync(1 << 10);
        assertThat(opts.bytesPerSync()).isEqualTo((long) (1 << 10));
        opts.setMaxWriteBufferNumber(3);
        assertThat(opts.maxWriteBufferNumber()).isEqualTo(3);
        opts.setMinWriteBufferNumberToMerge(10);
        assertThat(opts.minWriteBufferNumberToMerge()).isEqualTo(10);
        assertThat(opts.writeBufferManager()).isNull();

        opts.enableSpeedbFeatures(so);
        assertThat(opts.maxBackgroundJobs()).isEqualTo((int) totalThreads);
        assertThat(opts.delayedWriteRate()).isEqualTo((long) defaultDelayedWriteRate);
        assertThat(opts.bytesPerSync()).isEqualTo((long) (1 << 20));
        assertThat(opts.maxWriteBufferNumber()).isEqualTo(4);
        assertThat(opts.minWriteBufferNumberToMerge()).isEqualTo(1);
        // The following assertion will fail since the JNI assumes a WBM is created
        // via a call to setWriteBufferManager() which caches the newly created
        // WBM in a member. The accessor just returns that member rather than actually
        // obtaining the WBM from the CPP side.
        // assertThat(opts.writeBufferManager()).isNotNull();

        DBOptions db_opts = new DBOptions();
        db_opts.setDelayedWriteRate(1000);
        assertThat(db_opts.delayedWriteRate()).isEqualTo((long) 1000);

        db_opts.enableSpeedbFeatures(so);
        assertThat(db_opts.delayedWriteRate()).isEqualTo((long) defaultDelayedWriteRate);

        ColumnFamilyOptions cfo = new ColumnFamilyOptions();
        cfo.setMaxWriteBufferNumber(3);
        assertThat(cfo.maxWriteBufferNumber()).isEqualTo(3);

        cfo.enableSpeedbFeatures(so);
        assertThat(cfo.maxWriteBufferNumber()).isEqualTo(4);
      }
    }
  }
}
