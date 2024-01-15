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

import java.util.List;

public class SharedOptions extends RocksObject {
  public SharedOptions(final long capacity, final long total_threads) {
    super(newSharedOptions(capacity, total_threads));
  }

  public SharedOptions(final long capacity, final long total_threads, final long delayed_write_rate,
      final long bucket_size, final boolean use_merge) {
    super(newSharedOptions(capacity, total_threads, delayed_write_rate, bucket_size, use_merge));
  }

  public long getMaxWriteBufferManagerSize() {
    assert (isOwningHandle());
    return getMaxWriteBufferManagerSize(nativeHandle_);
  }

  public long getTotalThreads() {
    assert (isOwningHandle());
    return getTotalThreads(nativeHandle_);
  }

  public long getTotalRamSizeBytes() {
    assert (isOwningHandle());
    return getTotalRamSizeBytes(nativeHandle_);
  }

  public long getDelayedWriteRate() {
    assert (isOwningHandle());
    return getDelayedWriteRate(nativeHandle_);
  }

  public long getBucketSize() {
    assert (isOwningHandle());
    return getBucketSize(nativeHandle_);
  }

  public boolean isMergeMemtableSupported() {
    assert (isOwningHandle());
    return isMergeMemtableSupported(nativeHandle_);
  }

  private native static long newSharedOptions(final long capacity, final long total_threads,
      final long delayed_write_rate, final long bucket_size, final boolean use_merge);
  private native static long newSharedOptions(final long capacity, final long total_threads);
  @Override protected final native void disposeInternal(final long handle);
  private native static long getMaxWriteBufferManagerSize(final long handle);
  private native static long getTotalThreads(final long handle);
  private native static long getTotalRamSizeBytes(final long handle);
  private native static long getDelayedWriteRate(final long handle);
  private native static long getBucketSize(final long handle);
  private native static boolean isMergeMemtableSupported(final long handle);
}
