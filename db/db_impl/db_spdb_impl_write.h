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
#include <list>
#include <mutex>
#include <thread>
#include <vector>

#include "port/port.h"
#include "rocksdb/write_batch.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
struct WriteOptions;

struct WritesBatchList {
  std::list<WriteBatch*> wal_writes_;
  uint16_t elements_num_ = 0;
  uint64_t max_seq_ = 0;
  port::RWMutexWr buffer_write_rw_lock_;
  port::RWMutexWr write_ref_rwlock_;
  std::atomic<bool> need_sync_ = false;
  std::atomic<bool> switch_wb_ = false;
  std::atomic<bool> complete_batch_ = false;
  void Clear() {
    wal_writes_.clear();
    elements_num_ = 0;
    max_seq_ = 0;
    need_sync_ = false;
    switch_wb_ = false;
    complete_batch_ = false;
  }

 public:
  bool Add(WriteBatch* batch, const WriteOptions& write_options,
           bool* leader_batch);
  uint64_t GetMaxSeq() const { return max_seq_; }
  void WaitForPendingWrites();
  bool IsSwitchWBOccur() const { return switch_wb_.load(); }
  bool IsComplete() const { return complete_batch_.load(); }
  void WriteBatchComplete(bool leader_batch);
};

class SpdbWriteImpl {
 public:
  SpdbWriteImpl(DBImpl* db);

  ~SpdbWriteImpl();
  void SpdbFlushWriteThread();

  std::shared_ptr<WritesBatchList> Add(WriteBatch* batch,
                                       const WriteOptions& write_options,
                                       bool* leader_batch);
  std::shared_ptr<WritesBatchList> AddMerge(WriteBatch* batch,
                                            const WriteOptions& write_options,
                                            bool* leader_batch);
  void CompleteMerge();
  void Shutdown();
  void WaitForWalWriteComplete(void* list);
  void WriteBatchComplete(void* list, bool leader_batch);
  port::RWMutexWr& GetFlushRWLock() { return flush_rwlock_; }
  void Lock(bool is_read);
  void Unlock(bool is_read);

 public:
  void SwitchAndWriteBatchGroup(WritesBatchList* wb_list);
  void SwitchBatchGroupIfNeeded();
  void PublishedSeq();

  std::atomic<uint64_t> last_wal_write_seq_{0};

  std::list<std::shared_ptr<WritesBatchList>> wb_lists_;
  DBImpl* db_;
  std::atomic<bool> flush_thread_terminate_;
  std::mutex flush_thread_mutex_;
  std::condition_variable flush_thread_cv_;
  port::Mutex add_buffer_mutex_;
  port::RWMutexWr flush_rwlock_;
  std::thread flush_thread_;
  port::RWMutexWr wal_buffers_rwlock_;
  port::Mutex wal_write_mutex_;
  port::Mutex wb_list_mutex_;

  WriteBatch tmp_batch_;
};

}  // namespace ROCKSDB_NAMESPACE
