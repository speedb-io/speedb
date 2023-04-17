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
#include "db/db_impl/db_spdb_impl_write.h"

#include "db/db_impl/db_impl.h"
#include "db/write_batch_internal.h"
#include "logging/logging.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
#define MAX_ELEMENTS_IN_BATCH_GROUP 16
// add_buffer_mutex_ is held
bool WritesBatchList::Add(WriteBatch* batch, const WriteOptions& write_options,
                          bool* leader_batch) {
  elements_num_++;
  if (elements_num_ == MAX_ELEMENTS_IN_BATCH_GROUP) {
    switch_wb_.store(true);
  }
  const size_t seq_inc = batch->Count();
  max_seq_ = WriteBatchInternal::Sequence(batch) + seq_inc - 1;

  if (!write_options.disableWAL) {
    wal_writes_.push_back(batch);
  }
  if (write_options.sync && wal_writes_.size() != 0) {
    need_sync_ = true;
  }
  if (elements_num_ == 1) {
    // first wal batch . should take the buffer_write_rw_lock_ as write
    *leader_batch = true;
    buffer_write_rw_lock_.WriteLock();
  }
  write_ref_rwlock_.ReadLock();
  return switch_wb_.load();
}

void WritesBatchList::WriteBatchComplete(bool leader_batch) {
  // Batch was added to the memtable, we can release the memtable_ref.
  write_ref_rwlock_.ReadUnlock();
  if (leader_batch) {
    {
      // make sure all batches wrote to memtable (if needed) to be able progress
      // the version
      WriteLock wl(&write_ref_rwlock_);
    }
    complete_batch_.store(true);
    // wal write has been completed wal waiters will be released
    buffer_write_rw_lock_.WriteUnlock();
  } else {
    // wait wal write completed
    ReadLock rl(&buffer_write_rw_lock_);
  }
}

void WritesBatchList::WaitForPendingWrites() {
  // make sure all batches wrote to memtable (ifneeded) to be able progress the
  // version
  WriteLock wl(&write_ref_rwlock_);
}

void SpdbWriteImpl::WriteBatchComplete(void* list, bool leader_batch) {
  WritesBatchList* wb_list = static_cast<WritesBatchList*>(list);
  if (leader_batch) {
    SwitchAndWriteBatchGroup(wb_list);
  } else {
    wb_list->WriteBatchComplete(false);
  }
}

void SpdbWriteImpl::SpdbFlushWriteThread() {
  for (;;) {
    {
      std::unique_lock<std::mutex> lck(flush_thread_mutex_);
      auto duration = std::chrono::seconds(5);
      auto cv_status = flush_thread_cv_.wait_for(lck, duration);

      // Check if the wait stopped due to timing out.
      if (cv_status != std::cv_status::timeout ||
          flush_thread_terminate_.load()) {
        return;
      }
    }
    if (db_->CheckIfActionNeeded()) {
      // make sure no on the fly writes
      flush_rwlock_.WriteLock();
      db_->RegisterFlushOrTrim();
      flush_rwlock_.WriteUnlock();
    }
  }
}

SpdbWriteImpl::SpdbWriteImpl(DBImpl* db)
    : db_(db),
      flush_thread_terminate_(false),
      flush_thread_(&SpdbWriteImpl::SpdbFlushWriteThread, this) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  auto thread_handle = flush_thread_.native_handle();
  pthread_setname_np(thread_handle, "speedb:wflush");
#endif
#endif
  wb_lists_.push_back(std::make_shared<WritesBatchList>());
}

SpdbWriteImpl::~SpdbWriteImpl() {
  Shutdown();
  flush_thread_.join();
}

void SpdbWriteImpl::Shutdown() {
  { WriteLock wl(&flush_rwlock_); }
  {
    std::unique_lock<std::mutex> lck(flush_thread_mutex_);
    flush_thread_terminate_ = true;
  }
  flush_thread_cv_.notify_one();
}

bool DBImpl::CheckIfActionNeeded() {
  InstrumentedMutexLock l(&mutex_);

  if (total_log_size_ > GetMaxTotalWalSize()) {
    return true;
  }

  if (write_buffer_manager_->ShouldFlush()) {
    return true;
  }

  if (!flush_scheduler_.Empty()) {
    return true;
  }

  if (!trim_history_scheduler_.Empty()) {
    return true;
  }
  return false;
}

Status DBImpl::RegisterFlushOrTrim() {
  Status status;
  WriteContext write_context;
  InstrumentedMutexLock l(&mutex_);

  if (UNLIKELY(status.ok() && total_log_size_ > GetMaxTotalWalSize())) {
    status = SwitchWAL(&write_context);
  }

  if (UNLIKELY(status.ok() && write_buffer_manager_->ShouldFlush())) {
    status = HandleWriteBufferManagerFlush(&write_context);
  }

  if (UNLIKELY(status.ok() && !flush_scheduler_.Empty())) {
    status = ScheduleFlushes(&write_context);
  }

  if (UNLIKELY(status.ok() && !trim_history_scheduler_.Empty())) {
    status = TrimMemtableHistory(&write_context);
  }
  return status;
}

std::shared_ptr<WritesBatchList> SpdbWriteImpl::Add(
    WriteBatch* batch, const WriteOptions& write_options, bool* leader_batch) {
  MutexLock l(&add_buffer_mutex_);
  std::shared_ptr<WritesBatchList> current_wb = nullptr;
  {
    MutexLock wb_list_lock(&wb_list_mutex_);
    current_wb = wb_lists_.back();
  }
  const uint64_t sequence =
      db_->FetchAddLastAllocatedSequence(batch->Count()) + 1;
  WriteBatchInternal::SetSequence(batch, sequence);
  current_wb->Add(batch, write_options, leader_batch);
  /*if (need_switch_wb) {
    //create new wb
    wb_lists_.push_back(std::make_shared<WritesBatchList>());
  }*/
  return current_wb;
}

std::shared_ptr<WritesBatchList> SpdbWriteImpl::AddMerge(
    WriteBatch* batch, const WriteOptions& write_options, bool* leader_batch) {
  // thie will be released AFTER ths batch will be written to memtable!
  add_buffer_mutex_.Lock();
  std::shared_ptr<WritesBatchList> current_wb = nullptr;
  const uint64_t sequence =
      db_->FetchAddLastAllocatedSequence(batch->Count()) + 1;
  WriteBatchInternal::SetSequence(batch, sequence);
  // need to wait all prev batches completed to write to memetable and avoid
  // new batches to write to memetable before this one

  {
    MutexLock l(&wb_list_mutex_);
    for (std::list<std::shared_ptr<WritesBatchList>>::iterator iter =
             wb_lists_.begin();
         iter != wb_lists_.end(); ++iter) {
      (*iter)->WaitForPendingWrites();
    }
    current_wb = wb_lists_.back();
  }
  current_wb->Add(batch, write_options, leader_batch);

  return current_wb;
}
// release the add merge lock
void SpdbWriteImpl::CompleteMerge() { add_buffer_mutex_.Unlock(); }

void SpdbWriteImpl::Lock(bool is_read) {
  if (is_read) {
    flush_rwlock_.ReadLock();
  } else {
    flush_rwlock_.WriteLock();
  }
}

void SpdbWriteImpl::Unlock(bool is_read) {
  if (is_read) {
    flush_rwlock_.ReadUnlock();
  } else {
    flush_rwlock_.WriteUnlock();
  }
}

void SpdbWriteImpl::SwitchBatchGroupIfNeeded() {
  MutexLock l(&add_buffer_mutex_);
  MutexLock wb_list_lock(&wb_list_mutex_);
  // create new wb if needed
  // if (!wb_list->IsSwitchWBOccur()) {
  wb_lists_.push_back(std::make_shared<WritesBatchList>());
  //}
}

void SpdbWriteImpl::PublishedSeq() {
  uint64_t published_seq = 0;
  {
    MutexLock l(&wb_list_mutex_);
    std::list<std::shared_ptr<WritesBatchList>>::iterator iter =
        wb_lists_.begin();
    while (iter != wb_lists_.end()) {
      if ((*iter)->IsComplete()) {
        published_seq = (*iter)->GetMaxSeq();
        iter = wb_lists_.erase(iter);  // erase and go to next
      } else {
        break;
      }
    }
    if (published_seq != 0) {
      /*ROCKS_LOG_INFO(db_->immutable_db_options().info_log,
                     "PublishedSeq %" PRIu64, published_seq);*/
      db_->SetLastSequence(published_seq);
    }
  }
}

void SpdbWriteImpl::SwitchAndWriteBatchGroup(WritesBatchList* batch_group) {
  // take the wal write rw lock from protecting another batch group wal write
  IOStatus io_s;
  uint64_t offset = 0;
  uint64_t size = 0;
  // uint64_t start_offset = 0;
  // uint64_t total_size = 0;

  wal_write_mutex_.Lock();
  SwitchBatchGroupIfNeeded();
  /*ROCKS_LOG_INFO(db_->immutable_db_options().info_log,
                 "SwitchBatchGroup last batch group with %d batches and with "
                 "publish seq %" PRIu64,
                 batch_group->elements_num_, batch_group->GetMaxSeq());*/

  if (!batch_group->wal_writes_.empty()) {
    auto const& immutable_db_options = db_->immutable_db_options();
    StopWatch write_sw(immutable_db_options.clock, immutable_db_options.stats,
                       DB_WAL_WRITE_TIME);

    const WriteBatch* to_be_cached_state = nullptr;
    if (batch_group->wal_writes_.size() == 1 &&
        batch_group->wal_writes_.front()
            ->GetWalTerminationPoint()
            .is_cleared()) {
      WriteBatch* wal_batch = batch_group->wal_writes_.front();

      if (WriteBatchInternal::IsLatestPersistentState(wal_batch)) {
        to_be_cached_state = wal_batch;
      }
      io_s = db_->SpdbWriteToWAL(wal_batch, 1, to_be_cached_state,
                                 batch_group->need_sync_, &offset, &size);
    } else {
      uint64_t progress_batch_seq;
      size_t wal_writes = 0;
      WriteBatch* merged_batch = &tmp_batch_;
      for (const WriteBatch* batch : batch_group->wal_writes_) {
        if (wal_writes != 0 &&
            (progress_batch_seq != WriteBatchInternal::Sequence(batch))) {
          // this can happened if we have a batch group that consists no wal
          // writes... need to divide the wal writes when the seq is broken
          io_s =
              db_->SpdbWriteToWAL(merged_batch, wal_writes, to_be_cached_state,
                                  batch_group->need_sync_, &offset, &size);
          // reset counter and state
          tmp_batch_.Clear();
          wal_writes = 0;
          to_be_cached_state = nullptr;
          if (!io_s.ok()) {
            // TBD what todo with error
            break;
          }
        }
        if (wal_writes == 0) {
          // first batch seq to use when we will replay the wal after recovery
          WriteBatchInternal::SetSequence(merged_batch,
                                          WriteBatchInternal::Sequence(batch));
        }
        // to be able knowing the batch are in seq order
        progress_batch_seq =
            WriteBatchInternal::Sequence(batch) + batch->Count();
        Status s = WriteBatchInternal::Append(merged_batch, batch, true);
        // Always returns Status::OK.()
        if (!s.ok()) {
          assert(false);
        }
        if (WriteBatchInternal::IsLatestPersistentState(batch)) {
          // We only need to cache the last of such write batch
          to_be_cached_state = batch;
        }
        ++wal_writes;
      }
      if (wal_writes) {
        io_s = db_->SpdbWriteToWAL(merged_batch, wal_writes, to_be_cached_state,
                                   batch_group->need_sync_, &offset, &size);
        tmp_batch_.Clear();
      }
    }
  }
  wal_write_mutex_.Unlock();
  if (!io_s.ok()) {
    // TBD what todo with error
    ROCKS_LOG_ERROR(db_->immutable_db_options().info_log,
                    "Error write to wal!!! %s", io_s.ToString().c_str());
  }

  if (batch_group->need_sync_) {
    db_->SpdbSyncWAL(offset, size);
  }

  batch_group->WriteBatchComplete(true);
  /*ROCKS_LOG_INFO(db_->immutable_db_options().info_log,
                 "Complete batch group with publish seq %" PRIu64,
                 batch_group->GetMaxSeq());*/

  PublishedSeq();
}

Status DBImpl::SpdbWrite(const WriteOptions& write_options, WriteBatch* batch,
                         bool disable_memtable) {
  assert(batch != nullptr && WriteBatchInternal::Count(batch) > 0);
  StopWatch write_sw(immutable_db_options_.clock, immutable_db_options_.stats,
                     DB_WRITE);

  if (error_handler_.IsDBStopped()) {
    return error_handler_.GetBGError();
  }

  last_batch_group_size_ = WriteBatchInternal::ByteSize(batch);
  spdb_write_->Lock(true);

  if (write_options.disableWAL) {
    has_unpersisted_data_.store(true, std::memory_order_relaxed);
  }

  Status status;
  bool leader_batch = false;
  std::shared_ptr<WritesBatchList> list;
  if (batch->HasMerge()) {
    // need to wait all prev batches completed to write to memetable and avoid
    // new batches to write to memetable before this one
    list = spdb_write_->AddMerge(batch, write_options, &leader_batch);
  } else {
    list = spdb_write_->Add(batch, write_options, &leader_batch);
  }

  if (!disable_memtable) {
    bool concurrent_memtable_writes = !batch->HasMerge();
    status = WriteBatchInternal::InsertInto(
        batch, column_family_memtables_.get(), &flush_scheduler_,
        &trim_history_scheduler_, write_options.ignore_missing_column_families,
        0 /*recovery_log_number*/, this, concurrent_memtable_writes, nullptr,
        nullptr, seq_per_batch_, batch_per_txn_);
  }

  if (batch->HasMerge()) {
    spdb_write_->CompleteMerge();
  }

  // handle !status.ok()
  spdb_write_->WriteBatchComplete(list.get(), leader_batch);
  spdb_write_->Unlock(true);

  return status;
}

void DBImpl::SuspendSpdbWrites() {
  if (spdb_write_) {
    spdb_write_->Lock(false);
  }
}
void DBImpl::ResumeSpdbWrites() {
  if (spdb_write_) {
    // must release the db mutex lock before unlock spdb flush lock
    // to prevent deadlock!!! the db mutex will be acquired after the unlock
    mutex_.Unlock();
    spdb_write_->Unlock(false);
    // Lock again the db mutex as it was before we enterd this function
    mutex_.Lock();
  }
}

IOStatus DBImpl::SpdbSyncWAL(uint64_t offset, uint64_t size) {
  IOStatus io_s;
  StopWatch sw(immutable_db_options_.clock, stats_, WAL_FILE_SYNC_MICROS);
  {
    InstrumentedMutexLock l(&log_write_mutex_);
    log::Writer* log_writer = logs_.back().writer;
    io_s = log_writer->SyncRange(immutable_db_options_.use_fsync, offset, size);
    /*ROCKS_LOG_INFO(immutable_db_options().info_log,
                   "Complete SyncRange offset %" PRIu64 " size %" PRIu64,
                   offset, size);*/
  }
  if (io_s.ok() && !log_dir_synced_) {
    io_s = directories_.GetWalDir()->FsyncWithDirOptions(
        IOOptions(), nullptr,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    log_dir_synced_ = true;
    /*ROCKS_LOG_INFO(immutable_db_options().info_log, "Complete Sync dir");*/
  }
  return io_s;
}
IOStatus DBImpl::SpdbWriteToWAL(WriteBatch* merged_batch, size_t write_with_wal,
                                const WriteBatch* to_be_cached_state,
                                bool do_flush, uint64_t* offset,
                                uint64_t* size) {
  assert(merged_batch != nullptr || write_with_wal == 0);
  IOStatus io_s;

  const Slice log_entry = WriteBatchInternal::Contents(merged_batch);
  const uint64_t log_entry_size = log_entry.size();
  {
    InstrumentedMutexLock l(&log_write_mutex_);
    log::Writer* log_writer = logs_.back().writer;
    io_s = log_writer->AddRecordWithStartOffsetAndSize(log_entry, Env::IO_TOTAL,
                                                       do_flush, offset, size);
  }

  total_log_size_ += log_entry_size;
  // TODO(myabandeh): it might be unsafe to access alive_log_files_.back()
  // here since alive_log_files_ might be modified concurrently
  alive_log_files_.back().AddSize(log_entry_size);
  log_empty_ = false;

  if (to_be_cached_state != nullptr) {
    cached_recoverable_state_ = *to_be_cached_state;
    cached_recoverable_state_empty_ = false;
  }

  if (io_s.ok()) {
    InternalStats* stats = default_cf_internal_stats_;

    stats->AddDBStats(InternalStats::kIntStatsWalFileBytes, log_entry_size);
    RecordTick(stats_, WAL_FILE_BYTES, log_entry_size);
    stats->AddDBStats(InternalStats::kIntStatsWriteWithWal, write_with_wal);
    RecordTick(stats_, WRITE_WITH_WAL, write_with_wal);
  }

  return io_s;
}

}  // namespace ROCKSDB_NAMESPACE
