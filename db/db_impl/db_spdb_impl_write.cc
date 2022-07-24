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

// add_buffer_mutex_ is held
void SpdbWriteImpl::WritesBatchList::Add(WriteBatch* batch, bool disable_wal,
                                         bool* leader_batch) {
  const size_t seq_inc = batch->Count();
  max_seq_ = WriteBatchInternal::Sequence(batch) + seq_inc - 1;

  if (!disable_wal) {
    wal_writes_.push_back(batch);
  }
  if (empty_) {
    // first wal batch . should take the buffer_write_rw_lock_ as write
    *leader_batch = true;
    buffer_write_rw_lock_.WriteLock();
    empty_ = false;
  }
  write_ref_rwlock_.ReadLock();
}

void SpdbWriteImpl::WritesBatchList::WriteBatchComplete(bool leader_batch,
                                                        DBImpl* db) {
  // Batch was added to the memtable, we can release the memtable_ref.
  write_ref_rwlock_.ReadUnlock();
  if (leader_batch) {
    {
      // make sure all batches wrote to memtable (if needed) to be able progress
      // the version
      WriteLock wl(&write_ref_rwlock_);
    }
    db->SetLastSequence(max_seq_);
    // wal write has been completed wal waiters will be released
    buffer_write_rw_lock_.WriteUnlock();
  } else {
    // wait wal write completed
    ReadLock rl(&buffer_write_rw_lock_);
  }
}

void SpdbWriteImpl::WritesBatchList::WaitForPendingWrites() {
  // make sure all batches wrote to memtable (ifneeded) to be able progress the
  // version
  WriteLock wl(&write_ref_rwlock_);
}

void SpdbWriteImpl::WriteBatchComplete(void* list, bool leader_batch) {
  if (leader_batch) {
    SwitchAndWriteBatchGroup();
  } else {
    WritesBatchList* write_batch_list = static_cast<WritesBatchList*>(list);
    write_batch_list->WriteBatchComplete(false, db_);
  }
}

void SpdbWriteImpl::SpdbFlushWriteThread() {
  for (;;) {
    {
      std::unique_lock<std::mutex> lck(flush_thread_mutex_);
      flush_thread_cv_.wait(lck);
      if (flush_thread_terminate_.load()) {
        break;
      }
    }
    // make sure no on the fly writes
    WriteLock wl(&flush_rwlock_);
    db_->RegisterFlushOrTrim();
    action_needed_.store(false);
  }
}

SpdbWriteImpl::SpdbWriteImpl(DBImpl* db)
    : db_(db),
      flush_thread_terminate_(false),
      action_needed_(false),
      flush_thread_(&SpdbWriteImpl::SpdbFlushWriteThread, this) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  auto thread_handle = flush_thread_.native_handle();
  pthread_setname_np(thread_handle, "speedb:write_flush");
#endif
#endif
}

SpdbWriteImpl::~SpdbWriteImpl() {
  Shutdown();
  flush_thread_.join();
}

void SpdbWriteImpl::Shutdown() {
  {
    std::unique_lock<std::mutex> lck(flush_thread_mutex_);
    flush_thread_terminate_ = true;
  }
  flush_thread_cv_.notify_one();
}

void SpdbWriteImpl::NotifyIfActionNeeded() {
  bool need_notify = false;
  if (db_->CheckIfActionNeeded()) {
    bool tmp = false;
    std::unique_lock<std::mutex> lck(flush_thread_mutex_);
    need_notify = action_needed_.compare_exchange_strong(
        tmp, true, std::memory_order_relaxed);
  }
  if (need_notify) {
    flush_thread_cv_.notify_one();
  }
}

bool DBImpl::CheckIfActionNeeded() {
  InstrumentedMutexLock l(&mutex_);

  if (!single_column_family_mode_ && total_log_size_ > GetMaxTotalWalSize()) {
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

  if (UNLIKELY(status.ok() && !single_column_family_mode_ &&
               total_log_size_ > GetMaxTotalWalSize())) {
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

void* SpdbWriteImpl::Add(WriteBatch* batch, bool disable_wal,
                         bool* leader_batch) {
  MutexLock l(&add_buffer_mutex_);
  WritesBatchList& pending_list = GetActiveList();
  const uint64_t sequence =
      db_->FetchAddLastAllocatedSequence(batch->Count()) + 1;
  WriteBatchInternal::SetSequence(batch, sequence);
  pending_list.Add(batch, disable_wal, leader_batch);
  return &pending_list;
}

void* SpdbWriteImpl::AddMerge(WriteBatch* batch, bool disable_wal,
                              bool* leader_batch) {
  // thie will be released AFTER ths batch will be written to memtable!
  add_buffer_mutex_.Lock();
  const uint64_t sequence =
      db_->FetchAddLastAllocatedSequence(batch->Count()) + 1;
  WriteBatchInternal::SetSequence(batch, sequence);
  // need to wait all prev batches completed to write to memetable and avoid
  // new batches to write to memetable before this one
  for (uint32_t i = 0; i < kWalWritesContainers; i++) {
    wb_lists_[i].WaitForPendingWrites();
  }

  WritesBatchList& pending_list = GetActiveList();
  pending_list.Add(batch, disable_wal, leader_batch);
  return &pending_list;
}
// release the add merge lock
void SpdbWriteImpl::CompleteMerge() { add_buffer_mutex_.Unlock(); }

SpdbWriteImpl::WritesBatchList& SpdbWriteImpl::SwitchBatchGroup() {
  MutexLock l(&add_buffer_mutex_);
  WritesBatchList& batch_group = wb_lists_[active_buffer_index_];
  active_buffer_index_ = (active_buffer_index_ + 1) % wb_lists_.size();
  return batch_group;
}

void SpdbWriteImpl::SwitchAndWriteBatchGroup() {
  // take the wal write rw lock from protecting another batch group wal write
  MutexLock l(&wal_write_mutex_);
  NotifyIfActionNeeded();
  WritesBatchList& batch_group = SwitchBatchGroup();
  if (!batch_group.wal_writes_.empty()) {
    auto const& immutable_db_options = db_->immutable_db_options();
    StopWatch write_sw(immutable_db_options.clock, immutable_db_options.stats,
                       DB_WAL_WRITE_TIME);

    const WriteBatch* to_be_cached_state = nullptr;
    IOStatus io_s;
    if (batch_group.wal_writes_.size() == 1 && batch_group.wal_writes_.front()
                                                   ->GetWalTerminationPoint()
                                                   .is_cleared()) {
      WriteBatch* wal_batch = batch_group.wal_writes_.front();

      if (WriteBatchInternal::IsLatestPersistentState(wal_batch)) {
        to_be_cached_state = wal_batch;
      }
      io_s = db_->SpdbWriteToWAL(wal_batch, 1, to_be_cached_state);
    } else {
      uint64_t progress_batch_seq;
      size_t wal_writes = 0;
      WriteBatch* merged_batch = &tmp_batch_;
      for (const WriteBatch* batch : batch_group.wal_writes_) {
        if (wal_writes != 0 &&
            (progress_batch_seq != WriteBatchInternal::Sequence(batch))) {
          // this can happened if we have a batch group that consists no wal
          // writes... need to divide the wal writes when the seq is broken
          io_s =
              db_->SpdbWriteToWAL(merged_batch, wal_writes, to_be_cached_state);
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
        io_s =
            db_->SpdbWriteToWAL(merged_batch, wal_writes, to_be_cached_state);
        tmp_batch_.Clear();
      }
    }
    if (!io_s.ok()) {
      // TBD what todo with error
      ROCKS_LOG_ERROR(db_->immutable_db_options().info_log,
                      "Error write to wal!!! %s", io_s.ToString().c_str());
    }
  }

  batch_group.WriteBatchComplete(true, db_);
  batch_group.Clear();
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
  ReadLock rl(&spdb_write_.get()->GetFlushRWLock());

  if (write_options.disableWAL) {
    has_unpersisted_data_.store(true, std::memory_order_relaxed);
  }

  Status status;
  bool leader_batch = false;
  void* list;
  if (batch->HasMerge()) {
    // need to wait all prev batches completed to write to memetable and avoid
    // new batches to write to memetable before this one
    list =
        spdb_write_->AddMerge(batch, write_options.disableWAL, &leader_batch);
  } else {
    list = spdb_write_->Add(batch, write_options.disableWAL, &leader_batch);
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
  spdb_write_->WriteBatchComplete(list, leader_batch);

  return status;
}

void DBImpl::SuspendSpdbWrites() {
  if (spdb_write_) {
    spdb_write_.get()->GetFlushRWLock().WriteLock();
  }
}
void DBImpl::ResumeSpdbWrites() {
  if (spdb_write_) {
    // must release the db mutex lock before unlock spdb flush lock 
    // to prevent deadlock!!! the db mutex will be acquired after the unlock
    mutex_.Unlock();
    spdb_write_.get()->GetFlushRWLock().WriteUnlock();
    // Lock again the db mutex as it was before we enterd this function
    mutex_.Lock();
  } 
}


IOStatus DBImpl::SpdbWriteToWAL(WriteBatch* merged_batch, size_t write_with_wal,
                                const WriteBatch* to_be_cached_state) {
  assert(merged_batch != nullptr || write_with_wal == 0);
  IOStatus io_s;

  const Slice log_entry = WriteBatchInternal::Contents(merged_batch);
  const uint64_t log_entry_size = log_entry.size();

  {
    // TODO: Make sure accessing logs_ with only log writer mutex held is
    // enough and that we don't need to hold onto DB mutex as well
    InstrumentedMutexLock l(&log_write_mutex_);
    log::Writer* log_writer = logs_.back().writer;
    io_s = log_writer->AddRecord(log_entry);
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
