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

import org.junit.Test;
import org.rocksdb.CompactRangeOptions.BottommostLevelCompaction;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactRangeOptionsTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void exclusiveManualCompaction() {
    CompactRangeOptions opt = new CompactRangeOptions();
    boolean value = false;
    opt.setExclusiveManualCompaction(value);
    assertThat(opt.exclusiveManualCompaction()).isEqualTo(value);
    value = true;
    opt.setExclusiveManualCompaction(value);
    assertThat(opt.exclusiveManualCompaction()).isEqualTo(value);
  }

  @Test
  public void bottommostLevelCompaction() {
    CompactRangeOptions opt = new CompactRangeOptions();
    BottommostLevelCompaction value = BottommostLevelCompaction.kSkip;
    opt.setBottommostLevelCompaction(value);
    assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
    value = BottommostLevelCompaction.kForce;
    opt.setBottommostLevelCompaction(value);
    assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
    value = BottommostLevelCompaction.kIfHaveCompactionFilter;
    opt.setBottommostLevelCompaction(value);
    assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
    value = BottommostLevelCompaction.kForceOptimized;
    opt.setBottommostLevelCompaction(value);
    assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
  }

  @Test
  public void changeLevel() {
    CompactRangeOptions opt = new CompactRangeOptions();
    boolean value = false;
    opt.setChangeLevel(value);
    assertThat(opt.changeLevel()).isEqualTo(value);
    value = true;
    opt.setChangeLevel(value);
    assertThat(opt.changeLevel()).isEqualTo(value);
  }

  @Test
  public void targetLevel() {
    CompactRangeOptions opt = new CompactRangeOptions();
    int value = 2;
    opt.setTargetLevel(value);
    assertThat(opt.targetLevel()).isEqualTo(value);
    value = 3;
    opt.setTargetLevel(value);
    assertThat(opt.targetLevel()).isEqualTo(value);
  }

  @Test
  public void targetPathId() {
    CompactRangeOptions opt = new CompactRangeOptions();
    int value = 2;
    opt.setTargetPathId(value);
    assertThat(opt.targetPathId()).isEqualTo(value);
    value = 3;
    opt.setTargetPathId(value);
    assertThat(opt.targetPathId()).isEqualTo(value);
  }

  @Test
  public void allowWriteStall() {
    CompactRangeOptions opt = new CompactRangeOptions();
    boolean value = false;
    opt.setAllowWriteStall(value);
    assertThat(opt.allowWriteStall()).isEqualTo(value);
    value = true;
    opt.setAllowWriteStall(value);
    assertThat(opt.allowWriteStall()).isEqualTo(value);
  }

  @Test
  public void maxSubcompactions() {
    CompactRangeOptions opt = new CompactRangeOptions();
    int value = 2;
    opt.setMaxSubcompactions(value);
    assertThat(opt.maxSubcompactions()).isEqualTo(value);
    value = 3;
    opt.setMaxSubcompactions(value);
    assertThat(opt.maxSubcompactions()).isEqualTo(value);
  }

  @Test
  public void asyncCompletionCb() {
    CompactRangeOptions opt = new CompactRangeOptions();

    try (final AbstractCompactRangeCompletedCb completeCb = new TestCompactRangeCompletedCb()) {
      opt.setAsyncCompletionCb(completeCb);
    }
  }

  private static class TestCompactRangeCompletedCb extends AbstractCompactRangeCompletedCb {
    @Override
    public void CompactRangeCompleted(final Status completionStatus) {
      System.err.println("In TestCompactRangeCompletedCb::CompactRangeCompleted");
    }
  }
}
