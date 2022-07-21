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

  void Shutdown();

  void* Add(WriteBatch* batch, bool disable_wal, bool read_lock_for_memtable);

  void MemtableAddComplete(const void* batch_list);

  // Called from WAL writer thread (through DBImpl::SpdbWriteToWAL)
  uint64_t WalWriteComplete();

  void RegisterFlush();

  Status WaitForWalWrite(WriteBatch* batch);
  port::RWMutex& GetWriteRWLock() { return spdb_write_external_rwlock_; }

 private:
  uint64_t GetLastWalWriteSeq() {
    return last_wal_write_seq_.load(std::memory_order_acquire);
  }

  // Called from WAL writer thread
  bool SwitchBatchGroup();

  void SpdbWriteThread();

  // Called from WAL writer thread
  bool WaitForPendingWork(size_t written_buffers);

  struct WritesBatchList {
    std::list<WriteBatch*> wal_writes_;
    bool empty_ = true;
    uint64_t max_seq_ = 0;
    port::RWMutex memtable_complete_rwlock_;

    void Clear() {
      wal_writes_.clear();
      empty_ = true;
      max_seq_ = 0;
    }

    bool Add(WriteBatch* batch, bool disable_wal, bool read_lock_for_memtable);

    void MemtableAddComplete();

    bool IsEmpty()  const { return empty_; }
 
    uint64_t GetMaxSeq() const { return max_seq_; }

    void WaitForMemtableWriters();
  };

  WritesBatchList& GetWrittenList() {
    return wb_lists_[active_buffer_index_ ^ 1];
  }

  WritesBatchList& GetActiveList() { return wb_lists_[active_buffer_index_]; }

  static constexpr size_t kWalWritesContainers = 2;

  DBImpl* db_;

  std::atomic<uint64_t> last_wal_write_seq_{0};

  std::array<WritesBatchList, kWalWritesContainers> wb_lists_;
  size_t active_buffer_index_ = 0;

  bool terminate_ = false;

  port::Mutex mutex_;
  std::atomic<size_t> pending_buffers_{0};

  std::mutex spdb_write_thread_mutex_;
  std::atomic<size_t> threads_busy_waiting_{0};
  std::condition_variable spdb_write_thread_cv_;

  // This means we need to do some actions
  std::atomic<bool> flush_cfds_{false};
  std::mutex flush_mutex_;
  std::condition_variable flush_cv_;

  std::mutex notify_wal_write_mutex_;
  std::condition_variable notify_wal_write_cv_;

  // acquire write make sure there is no on the fly writes
  port::RWMutex spdb_write_external_rwlock_; 
  // acquire write make sure new batch will wait until register flush                 
  port::RWMutex spdb_write_register_flush_rwlock_;     

  std::thread spdb_write_thread_;
  WriteBatch tmp_batch_;
};

}  // namespace ROCKSDB_NAMESPACE
