// Copyright 2022 SpeeDB Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

class SpdbWriteImpl {
 public:
  SpdbWriteImpl(DBImpl* db);

  ~SpdbWriteImpl();
  void SpdbFlushWriteThread();
  void* Add(WriteBatch* batch, const WriteOptions& write_options, bool* leader_batch);
  void* AddMerge(WriteBatch* batch, const WriteOptions& write_options, bool* leader_batch);
  void CompleteMerge();
  void Shutdown();
  void NotifyIfActionNeeded();
  void WaitForWalWriteComplete(void* list);
  void SwitchAndWriteBatchGroup();
  void WriteBatchComplete(void* list, bool leader_batch);
  port::RWMutexWr& GetFlushRWLock() { return flush_rwlock_; }
  void Lock(bool is_read);
  void Unlock(bool is_read);

 private:
  struct WritesBatchList {
    std::list<WriteBatch*> wal_writes_;
    uint64_t pulished_seq_ = 0;
    port::RWMutexWr buffer_write_rw_lock_;
    port::RWMutexWr write_ref_rwlock_;
    bool empty_ = true;
    std::atomic<bool> need_sync_ = false;
    void Clear() {
      wal_writes_.clear();
      pulished_seq_ = 0;
      empty_ = true;
      need_sync_ = false;
    }

    void Add(WriteBatch* batch, const WriteOptions& write_options, bool* leader_batch);
    void WaitForPendingWrites();

    void WriteBatchComplete(bool leader_batch, DBImpl* db);
  };

  WritesBatchList& SwitchBatchGroup();

  WritesBatchList& GetActiveList() { return wb_lists_[active_buffer_index_]; }
  static constexpr size_t kWalWritesContainers = 2;

  std::array<WritesBatchList, kWalWritesContainers> wb_lists_;
  size_t active_buffer_index_ = 0;

  DBImpl* db_;
  std::atomic<bool> flush_thread_terminate_;
  std::mutex flush_thread_mutex_;
  std::condition_variable flush_thread_cv_;
  port::Mutex add_buffer_mutex_;
  port::RWMutexWr flush_rwlock_;
  std::thread flush_thread_;
  port::RWMutexWr wal_buffers_rwlock_;
  port::Mutex wal_write_mutex_;
  WriteBatch tmp_batch_;
};

}  // namespace ROCKSDB_NAMESPACE
