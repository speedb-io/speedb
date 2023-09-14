 // Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;


public abstract class SharedOptions extends RocksObject {
  protected SharedOptions(final long nativeHandle) {
    super(nativeHandle);
  }

   protected SharedOptions(final long nativeHandle, final long total_ram_size_bytes, final long total_threads,
                             final long delayed_write_rate) {
    super(nativeHandle);
  }
}
