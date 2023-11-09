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

import java.util.Arrays;
import java.util.Random;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;

public class SharedOptionsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void shortConstructor() {
    long totalRamSize=100*1024*1024*1024;
    long totalThreads=10;
    long delayedWriteRate = 256*1024*1024;;
    long bucketSize = 50000;
    try (final SharedOptions so = new SharedOptions(totalRamSize, totalThreads)) {
	assertThat(so.getTotalThreads()).isEqualTo(totalThreads);
	assertThat(so.getTotalRamSizeBytes()).isEqualTo(totalRamSize);
    }
  }
    
  @Test
  public void altConstructor() {
    long totalRamSize=100*1024*1024*1024;
    long totalThreads=10;
    long delayedWriteRate = 256*1024*1024;;
    long bucketSize = 50000;
    try (final SharedOptions so = new SharedOptions(totalRamSize, totalThreads,
						    delayedWriteRate, bucketSize, true)) {
	assertThat(so.getTotalThreads()).isEqualTo(totalThreads);
	assertThat(so.getTotalRamSizeBytes()).isEqualTo(totalRamSize);
	assertThat(so.getDelayedWriteRate()).isEqualTo(delayedWriteRate);
	assertThat(so.getMaxWriteBufferManagerSize()).isEqualTo(totalRamSize/4);
	assertThat(so.getBucketSize()).isEqualTo(bucketSize);
	assertThat(so.isMergeMemtableSupported()).isEqualTo(true);
    }
  }

  @Test
  public void enableSpeedb() {
    try (final Options opts = new Options()) {
      long totalRamSize=100*1024*1024*1024;
      long totalThreads=10;
      try (final SharedOptions so = new SharedOptions(totalRamSize, totalThreads)) {
	  opts.enableSpeedbFeatures(so);
      }
    }
  }
}
