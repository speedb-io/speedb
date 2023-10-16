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
  public SharedOptions(
      final long capacity, final long total_threads, final long delayed_write_rate) {
    super(newSharedOptions(capacity, total_threads, delayed_write_rate));
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

  public void increaseWriteBufferSize(long increase_by) {
    assert (isOwningHandle());
    increaseWriteBufferSize(nativeHandle_, increase_by);
  }

  public SharedOptions setCache(final Cache cache) {
    assert (isOwningHandle());
    setCache(nativeHandle_, cache.nativeHandle_);
    return this;
  }

  public SharedOptions setWriteBufferManager(final WriteBufferManager writeBufferManager) {
    assert (isOwningHandle());
    setWriteBufferManager(nativeHandle_, writeBufferManager.nativeHandle_);
    return this;
  }

  public SharedOptions setEnv(final Env env) {
    assert (isOwningHandle());
    setEnv(nativeHandle_, env.nativeHandle_);
    return this;
  }

  public SharedOptions setRateLimiter(final RateLimiter rateLimiter) {
    assert (isOwningHandle());
    setRateLimiter(nativeHandle_, rateLimiter.nativeHandle_);
    return this;
  }

  public SharedOptions setSstFileManager(final SstFileManager sstFileManager) {
    assert (isOwningHandle());
    setSstFileManager(nativeHandle_, sstFileManager.nativeHandle_);
    return this;
  }

  public SharedOptions setLogger(final Logger logger) {
    assert (isOwningHandle());
    setLogger(nativeHandle_, logger.nativeHandle_);
    return this;
  }

  public SharedOptions setListeners(final List<AbstractEventListener> listeners) {
    assert (isOwningHandle());
    setEventListeners(nativeHandle_, RocksCallbackObject.toNativeHandleList(listeners));
    return this;
  }

  private native static long newSharedOptions(
      final long capacity, final long total_threads, final long delayed_write_rate);
  @Override protected final native void disposeInternal(final long handle);
  private native static long getTotalThreads(final long handle);
  private native static long getTotalRamSizeBytes(final long handle);
  private native static long getDelayedWriteRate(final long handle);
  private native static void increaseWriteBufferSize(final long handle, final long increase_by);

  private native void setCache(final long handle, final long cacheHandle);
  private native void setWriteBufferManager(final long handle, final long wbmHandle);
  private native void setEnv(final long handle, final long envHandle);
  private native void setRateLimiter(long handle, long rateLimiterHandle);
  private native void setSstFileManager(final long handle, final long sstFileManagerHandle);
  private native void setLogger(long handle, long loggerHandle);
  private static native void setEventListeners(
      final long handle, final long[] eventListenerHandles);
}
