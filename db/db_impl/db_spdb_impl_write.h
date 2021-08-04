//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "port/port.h"
#include "rocksdb/write_batch.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class SystemClock;
class Statistics;
class WriteBatch;

class SpdbWriteImpl {
 public:
  SpdbWriteImpl(DBImpl* db);

  ~SpdbWriteImpl();
  void SpdbFlushWriteThread();
  void* Add(WriteBatch* batch, bool disable_wal, bool disable_memtable,
            bool* first_batch);

  void Shutdown();

  void NotifyFlushNeeded();
  void RegisterFlushIfNeeded();

  void WaitForWalWriteComplete(void* list);

  void SwitchAndWriteBatchGroup();

  void WriteBatchComplete(void* list, bool first_batch, bool disable_wal,
                          bool disable_memtable);

  void Lock();
  void Unlock();

  port::RWMutex& GetFlushRWLock() { return flush_rwlock_; }

 private:
  struct WritesBatchList {
    std::list<WriteBatch*> wal_writes_;
    uint64_t max_seq_ = 0;
    port::RWMutex buffer_write_rw_lock_;
    port::RWMutex memtable_ref_rwlock_;
    void Clear() {
      wal_writes_.clear();
      max_seq_ = 0;
    }

    void Add(WriteBatch* batch, bool disable_wal, bool disable_memtable,
             bool* first_batch);
    uint64_t GetMaxSeq() const { return max_seq_; }

    void WriteBatchComplete(bool first_batch, bool disable_wal = false,
                            bool disable_memtable = false);
  };

  WritesBatchList& GetWrittenList() {
    return wb_lists_[active_buffer_index_ ^ 1];
  }

  WritesBatchList& SwitchBatchGroup();

  WritesBatchList& GetActiveList() { return wb_lists_[active_buffer_index_]; }
  static constexpr size_t kWalWritesContainers = 2;

  std::atomic<uint64_t> last_wal_write_seq_{0};

  std::array<WritesBatchList, kWalWritesContainers> wb_lists_;
  size_t active_buffer_index_ = 0;

  DBImpl* db_;
  std::thread flush_thread_;
  std::atomic<bool> flush_thread_terminate_;
  std::mutex flush_thread_mutex_;
  std::condition_variable flush_thread_cv_;
  std::atomic<bool> flush_needed_;  // this means we need to do some actions
  port::Mutex add_buffer_mutex_;

  port::RWMutex flush_rwlock_;
  port::RWMutex wal_buffers_rwlock_;
  port::RWMutex wal_write_rwlock_;
  WriteBatch tmp_batch_;
};

}  // namespace ROCKSDB_NAMESPACE
