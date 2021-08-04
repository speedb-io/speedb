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
#include "db/error_handler.h"
#include "logging/logging.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// add_buffer_mutex_ is held
void SpdbWriteImpl::WritesBatchList::Add(WriteBatch* batch,
                                         const WriteOptions& write_options,
                                         bool* leader_batch,
                                         bool with_barrier) {
  const size_t seq_inc = batch->Count();
  // note we progressed the seq eventhough seq per batch is set .
  // in the worse case we just make a seq jump that are not in used..
  pulished_seq_ = WriteBatchInternal::Sequence(batch) + seq_inc - 1;
  if (WriteBatchInternal::Count(batch) > 0) {
    batch_group_size_in_bytes_ += WriteBatchInternal::ByteSize(batch);
    batch_group_size_ += batch->Count();
  }
  if (!write_options.disableWAL) {
    if (with_barrier) {
      wal_writes_.push_back(&barrier_batch);
    }
    wal_writes_.push_back(batch);
  }
  if (write_options.sync) {
    need_sync_ = true;
  }
  if (empty_) {
    // first wal batch . should take the buffer_write_rw_lock_ as write
    *leader_batch = true;
    buffer_write_rw_lock_.WriteLock();
    batch_group_rwlock_.WriteLock();
    empty_ = false;
  }
  write_ref_rwlock_.ReadLock();
  // in most cases it wont be used to force anything. (only on case of rollback)
  roll_back_write_ref_rwlock_.ReadLock();
}

Status DBImpl::RollbackBatch(WriteBatch* batch) {
  return WriteBatchInternal::Rollback(
      batch, column_family_memtables_.get(), &flush_scheduler_,
      &trim_history_scheduler_, false, 0 /*recovery_log_number*/, this, true,
      nullptr, nullptr, seq_per_batch_, batch_per_txn_);
}

Status SpdbWriteImpl::WritesBatchList::InternalWriteBatchComplete(
    DBImpl* db, BatchWriteType batch_write_type, WriteBatch* batch,
    Status batch_status) {
  // get the status of the batch group write...
  // if not ok and this batch perform a success memtable write , roll back
  // should be performed
  Status status = batch_status;
  if (!status_.ok() && batch_write_type != kMemtableOnly && batch_status.ok()) {
    if (WriteBatchInternal::Sequence(batch) >= roll_back_seq_) {
      status = status_;
      if (batch_write_type == kWalAndMemtable) {
        // call rollback
        if (!db->RollbackBatch(batch).ok()) {
          // failed to rollback!!! we have data integrity!!!!
          ROCKS_LOG_ERROR(db->immutable_db_options().info_log,
                          "Failed to rollback!!! we have data integrity!!!! "
                          "batch seq %" PRIu64,
                          WriteBatchInternal::Sequence(batch));
          status = Status::Corruption();
        }
      }
    }
  }
  roll_back_write_ref_rwlock_.ReadUnlock();
  return status;
}

void SpdbWriteImpl::WritesBatchList::WriteBatchLeaderPreComplete() {
  // Batch was added to the memtable, we must release the memtable_ref
  // to prevent deadlock when there are merge batches
  write_ref_rwlock_.ReadUnlock();
}

Status SpdbWriteImpl::WritesBatchList::WriteBatchLeaderComplete(
    DBImpl* db, BatchWriteType batch_write_type, WriteBatch* batch,
    Status batch_status) {
  {
    // make sure all batches wrote to memtable (if needed) to be able progress
    // the version
    WriteLock wl(&write_ref_rwlock_);
  }
  // wal write has been completed wal waiters will be released
  buffer_write_rw_lock_.WriteUnlock();

  // get the status of the batch group write...
  // if not ok and this batch perform a success memtable write , roll back
  // should be performed
  Status status =
      InternalWriteBatchComplete(db, batch_write_type, batch, batch_status);

  // batch group mission has been completed wal waiters will be released
  if (!status_.ok()) {
    // make sure all batches did rollback to memtable (if needed) to be able
    // progress the version
    WriteLock wl(&roll_back_write_ref_rwlock_);
  }
  // note ! we published the batch group seq  number eventhough we had
  // rollback!!! this  is ok since the rollbacked batches are signed as rollback
  db->SetLastSequence(pulished_seq_);
  batch_group_rwlock_.WriteUnlock();
  /*auto default_stat = db->GetDefaultStat();
  auto stats = db->GetStatistic();
  default_stat->AddDBStats(InternalStats::kIntStatsNumKeysWritten,
                           batch_group_size_);
  RecordTick(stats, NUMBER_KEYS_WRITTEN, batch_group_size_);
  default_stat->AddDBStats(InternalStats::kIntStatsBytesWritten,
                           batch_group_size_in_bytes_);
  RecordTick(stats, BYTES_WRITTEN, batch_group_size_in_bytes_);
  default_stat->AddDBStats(InternalStats::kIntStatsWriteDoneBySelf, 1);
  RecordTick(stats, WRITE_DONE_BY_SELF);
  RecordInHistogram(stats, BYTES_PER_WRITE, batch_group_size_in_bytes_);*/
  return status;
}

Status SpdbWriteImpl::WritesBatchList::WriteBatchComplete(
    DBImpl* db, BatchWriteType batch_write_type, WriteBatch* batch,
    Status batch_status) {
  // Batch was added to the memtable, we can release the memtable_ref.
  write_ref_rwlock_.ReadUnlock();
  {
    // wait wal write completed
    ReadLock rl(&buffer_write_rw_lock_);
  }
  // get the status of the batch group write...
  // if not ok and this batch perform a success memtable write , roll back
  // should be performed
  Status status =
      InternalWriteBatchComplete(db, batch_write_type, batch, batch_status);
  {
    // wait batch group mission completed
    ReadLock rl(&batch_group_rwlock_);
  }
  return status;
}

void SpdbWriteImpl::WritesBatchList::WaitForPendingWrites() {
  // make sure all batches wrote to memtable (ifneeded) to be able progress the
  // version
  WriteLock wl(&write_ref_rwlock_);
}

Status SpdbWriteImpl::WriteBatchLeaderComplete(void* list,
                                               BatchWriteType batch_write_type,
                                               WriteBatch* batch,
                                               Status batch_status) {
  static_cast<WritesBatchList*>(list)->WriteBatchLeaderPreComplete();
  return SwitchAndWriteBatchGroup(batch_write_type, batch, batch_status);
}

Status SpdbWriteImpl::WriteBatchComplete(void* list,
                                         BatchWriteType batch_write_type,
                                         WriteBatch* batch,
                                         Status batch_status) {
  return static_cast<WritesBatchList*>(list)->WriteBatchComplete(
      db_, batch_write_type, batch, batch_status);
}


SpdbWriteImpl::SpdbWriteImpl(DBImpl* db) : db_(db) {}

SpdbWriteImpl::~SpdbWriteImpl() { Shutdown(); }

void SpdbWriteImpl::Shutdown() { WriteLock wl(&flush_rwlock_); }

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

  if (write_controller_.IsStopped() || write_controller_.NeedsDelay()) {
    return true;
  }

  if (!trim_history_scheduler_.Empty()) {
    return true;
  }

  return false;
}

Status DBImpl::SpdbDelayWrite() {
  uint64_t time_delayed = 0;
  bool delayed = false;
  {
    StopWatch sw(immutable_db_options_.clock, stats_, WRITE_STALL,
                 &time_delayed);
    uint64_t delay = write_controller_.GetDelay(immutable_db_options_.clock,
                                                last_batch_group_size_);
    if (delay > 0) {
      TEST_SYNC_POINT("DBImpl::DelayWrite:Sleep");
      mutex_.Unlock();
      // We will delay the write until we have slept for `delay` microseconds
      // or we don't need a delay anymore. We check for cancellation every 1ms
      // (slightly longer because WriteController minimum delay is 1ms, in
      // case of sleep imprecision, rounding, etc.)
      const uint64_t kDelayInterval = 1001;
      uint64_t stall_end = sw.start_time() + delay;
      while (write_controller_.NeedsDelay()) {
        if (immutable_db_options_.clock->NowMicros() >= stall_end) {
          // We already delayed this write `delay` microseconds
          break;
        }

        delayed = true;
        // Sleep for 0.001 seconds
        immutable_db_options_.clock->SleepForMicroseconds(kDelayInterval);
      }
      mutex_.Lock();
    }
  }
  assert(!delayed);
  if (delayed) {
    default_cf_internal_stats_->AddDBStats(
        InternalStats::kIntStatsWriteStallMicros, time_delayed);
    RecordTick(stats_, STALL_MICROS, time_delayed);
  }

  // If DB is not in read-only mode and write_controller is not stopping
  // writes, we can ignore any background errors and allow the write to
  // proceed
  Status s;
  if (write_controller_.IsStopped()) {
    // If writes are still stopped, it means we bailed due to a background
    // error
    s = Status::Incomplete(error_handler_.GetBGError().ToString());
  }
  if (error_handler_.IsDBStopped()) {
    s = error_handler_.GetBGError();
  }
  return s;
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
  PERF_TIMER_GUARD(write_pre_and_post_process_time);

  if (UNLIKELY(status.ok() && (write_controller_.IsStopped() ||
                               write_controller_.NeedsDelay()))) {
    PERF_TIMER_STOP(write_pre_and_post_process_time);
    PERF_TIMER_GUARD(write_delay_time);
    // We don't know size of curent batch so that we always use the size
    // for previous one. It might create a fairness issue that expiration
    // might happen for smaller writes but larger writes can go through.
    // Can optimize it if it is an issue.
    status = SpdbDelayWrite();
    PERF_TIMER_START(write_pre_and_post_process_time);
  }

  if (UNLIKELY(status.ok() && !trim_history_scheduler_.Empty())) {
    status = TrimMemtableHistory(&write_context);
  }
  return status;
}

void* SpdbWriteImpl::Add(WriteBatch* batch, const WriteOptions& write_options,
                         bool* leader_batch) {
  MutexLock l(&add_buffer_mutex_);
  WritesBatchList& pending_list = GetActiveList();
  const uint64_t sequence =
      db_->FetchAddLastAllocatedSequence(batch->Count()) + 1;
  WriteBatchInternal::SetSequence(batch, sequence);
  pending_list.Add(batch, write_options, leader_batch);
  return &pending_list;
}

void* SpdbWriteImpl::AddWithBlockParallel(WriteBatch* batch,
                                          const WriteOptions& write_options,
                                          bool allow_write_batching,
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
  pending_list.Add(batch, write_options, leader_batch, !allow_write_batching);
  return &pending_list;
}
// release the block parallel
void SpdbWriteImpl::UnBlockParallel() { add_buffer_mutex_.Unlock(); }

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

SpdbWriteImpl::WritesBatchList* SpdbWriteImpl::SwitchBatchGroup() {
  MutexLock l(&add_buffer_mutex_);
  WritesBatchList* batch_group = &wb_lists_[active_buffer_index_];
  active_buffer_index_ = (active_buffer_index_ + 1) % wb_lists_.size();
  return batch_group;
}

Status SpdbWriteImpl::SwitchAndWriteBatchGroup(BatchWriteType batch_write_type,
                                               WriteBatch* batch,
                                               Status batch_status) {
  Status status;
  {
    WritesBatchList* batch_group = nullptr;
    // this is incase we are in divided wal writes so we will be able to know
    // to what seq start perform roll back
    uint64_t potential_roll_back_batch_seq = 0;
    // take the wal write rw lock from protecting another batch group wal write
    MutexLock l(&wal_write_mutex_);
    batch_group = SwitchBatchGroup();
    db_->UpdateLastGroupBatchSize(batch_group->batch_group_size_in_bytes_);
    if (!batch_group->wal_writes_.empty()) {
      auto const& immutable_db_options = db_->immutable_db_options();
      StopWatch write_sw(immutable_db_options.clock, immutable_db_options.stats,
                         DB_WAL_WRITE_TIME);

      const WriteBatch* to_be_cached_state = nullptr;
      IOStatus io_s;
      if (batch_group->wal_writes_.size() == 1 &&
          batch_group->wal_writes_.front()
              ->GetWalTerminationPoint()
              .is_cleared()) {
        WriteBatch* wal_batch = batch_group->wal_writes_.front();

        if (WriteBatchInternal::IsLatestPersistentState(wal_batch)) {
          to_be_cached_state = wal_batch;
        }
        potential_roll_back_batch_seq = WriteBatchInternal::Sequence(wal_batch);
        io_s = db_->SpdbWriteToWAL(wal_batch, 1, to_be_cached_state);
      } else {
        uint64_t progress_batch_seq;
        size_t wal_writes = 0;
        bool barrier_batch = false;
        WriteBatch* merged_batch = &tmp_batch_;
        for (const WriteBatch* wal_batch : batch_group->wal_writes_) {
          if (WriteBatchInternal::Sequence(wal_batch) == 0) {
            // this is a barrier batch. need write to wal only it
            barrier_batch = true;
            // need to go to the next batch to write it to wal
            continue;
          }
          if ((wal_writes != 0 && (progress_batch_seq !=
                                   WriteBatchInternal::Sequence(wal_batch))) ||
              barrier_batch) {
            // this can happened if we have a batch group that consists no wal
            // writes... need to divide the wal writes when the seq is broken
            io_s = db_->SpdbWriteToWAL(merged_batch, wal_writes,
                                       to_be_cached_state);
            // reset counter and state
            tmp_batch_.Clear();
            wal_writes = 0;
            barrier_batch = false;
            to_be_cached_state = nullptr;
            if (!io_s.ok()) {
              break;
            }
          }
          if (wal_writes == 0) {
            potential_roll_back_batch_seq =
                WriteBatchInternal::Sequence(wal_batch);
            // first batch seq to use when we will replay the wal after recovery
            WriteBatchInternal::SetSequence(
                merged_batch, WriteBatchInternal::Sequence(wal_batch));
          }
          // to be able knowing the batch are in seq order
          progress_batch_seq =
              WriteBatchInternal::Sequence(wal_batch) + wal_batch->Count();
          status = WriteBatchInternal::Append(merged_batch, wal_batch, true);
          // Always returns Status::OK.()
          if (!status.ok()) {
            assert(false);
          }
          if (WriteBatchInternal::IsLatestPersistentState(wal_batch)) {
            // We only need to cache the last of such write batch
            to_be_cached_state = wal_batch;
          }
          ++wal_writes;
        }
        if (wal_writes) {
          io_s =
              db_->SpdbWriteToWAL(merged_batch, wal_writes, to_be_cached_state);
          tmp_batch_.Clear();
        }
      }
      status = io_s;
      if (status.ok()) {
        if (batch_group->need_sync_) {
          status = db_->SpdbSyncWAL();
          if (!status.ok()) {
            WriteBatch* wal_batch = batch_group->wal_writes_.front();
            potential_roll_back_batch_seq =
                WriteBatchInternal::Sequence(wal_batch);
            ROCKS_LOG_ERROR(db_->immutable_db_options().info_log,
                            "Error sync to wal!!! %s", io_s.ToString().c_str());
          }
        }
      }
    }
    if (!status.ok()) {
      ROCKS_LOG_ERROR(db_->immutable_db_options().info_log,
                      "Error write to wal!!! %s", status.ToString().c_str());
      batch_group->SetRollback(potential_roll_back_batch_seq, status);
    }

    status = batch_group->WriteBatchLeaderComplete(db_, batch_write_type, batch,
                                                   batch_status);
    batch_group->Clear();
    return status;
  }
}
Status DBImpl::SpdbWrite(const WriteOptions& write_options, WriteBatch* batch,
                         WriteCallback* callback, uint64_t* log_used,
                         bool disable_memtable, uint64_t* seq_used) {
  assert(batch != nullptr);
  if (!immutable_db_options_.allow_concurrent_memtable_write) {
    return Status::NotSupported(
        "allow_concurrent_memtable_write MUST be set with spdb WF and with "
        "supported memtable! (skip_list/hash_spd");
  }
  StopWatch write_sw(immutable_db_options_.clock, immutable_db_options_.stats,
                     DB_WRITE);
  if (error_handler_.IsDBStopped()) {
    return error_handler_.GetBGError();
  }

  Status status;
  bool allow_write_batching = true;
  if (callback) {
    status = callback->Callback(this);
    if (!status.ok()) {
      return status;
    }
    allow_write_batching = callback->AllowWriteBatching();
  }

  spdb_write_->Lock(true);
  if (shutdown_initiated_) {
    // if the DB in shutdown we must exit immediately and ignore new writes with
    // error
    spdb_write_->Unlock(true);
    return Status::ShutdownInProgress();
  }
  if (log_used) {
    *log_used = logfile_number_;
  }

  if (write_options.disableWAL) {
    has_unpersisted_data_.store(true, std::memory_order_relaxed);
  }

  bool leader_batch = false;
  void* list;
  bool prevent_parallel_batches_write =
      batch->HasMerge() || !allow_write_batching;
  if (prevent_parallel_batches_write) {
    // need to wait all prev batches completed to write to memetable and avoid
    // new batches to write to memetable before this one
    list = spdb_write_->AddWithBlockParallel(
        batch, write_options, allow_write_batching, &leader_batch);
  } else {
    list = spdb_write_->Add(batch, write_options, &leader_batch);
  }
  if (seq_used) {
    *seq_used = WriteBatchInternal::Sequence(batch);
  }

  SpdbWriteImpl::BatchWriteType batch_write_type = SpdbWriteImpl::kNone;

  if (!disable_memtable && !write_options.disableWAL) {
    batch_write_type = SpdbWriteImpl::kWalAndMemtable;
  } else {
    if (write_options.disableWAL) {
      batch_write_type = SpdbWriteImpl::kMemtableOnly;
    } else {
      batch_write_type = SpdbWriteImpl::kWalOnly;
    }
  }
  if (!disable_memtable) {
    bool concurrent_memtable_writes = !batch->HasMerge();
    status = WriteBatchInternal::InsertInto(
        batch, column_family_memtables_.get(), &flush_scheduler_,
        &trim_history_scheduler_, write_options.ignore_missing_column_families,
        0 /*recovery_log_number*/, this, concurrent_memtable_writes, nullptr,
        nullptr, seq_per_batch_, batch_per_txn_);
    if (!status.ok()) {
      // write to memtable failed . need to remember that error and return to
      // user. also set in the batch group to be able notify the error BG task
      spdb_write_->SetMemtableWriteError(list);
    }
  }

  if (prevent_parallel_batches_write) {
    spdb_write_->UnBlockParallel();
  }
  if (leader_batch) {
    // handle !status.ok() inside
    status = spdb_write_->WriteBatchLeaderComplete(list, batch_write_type,
                                                   batch, status);
    if (spdb_write_->GetMemtableWriteError(list)) {
      MemTableInsertStatusCheck(Status::Incomplete());
    }
  } else {
    status =
        spdb_write_->WriteBatchComplete(list, batch_write_type, batch, status);
  }

  spdb_write_->Unlock(true);
  if (leader_batch) {
    if (CheckIfActionNeeded()) {
      spdb_write_->Lock(false);
      if (!shutdown_initiated_) {
        // if the DB in shutdown we dont care about releasing resources
        RegisterFlushOrTrim();
      }
      spdb_write_->Unlock(false);
    }
  }

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

Status DBImpl::SpdbSyncWAL() {
  IOStatus io_s;
  // ROCKS_LOG_INFO(immutable_db_options_.info_log, "Sync writes!");

  StopWatch sw(immutable_db_options_.clock, stats_, WAL_FILE_SYNC_MICROS);
  log::Writer* log_writer = logs_.back().writer;
  io_s = log_writer->file()->Sync(immutable_db_options_.use_fsync);
  if (io_s.ok() && !log_dir_synced_) {
    io_s = directories_.GetWalDir()->FsyncWithDirOptions(
        IOOptions(), nullptr,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
  }
  Status status = io_s;
  {
    InstrumentedMutexLock l(&mutex_);
    if (status.ok()) {
      status = MarkLogsSynced(logfile_number_, true);
    } else {
      MarkLogsNotSynced(logfile_number_);
    }
  }

  return status;
}
IOStatus DBImpl::SpdbWriteToWAL(WriteBatch* merged_batch, size_t write_with_wal,
                                const WriteBatch* to_be_cached_state) {
  assert(merged_batch != nullptr || write_with_wal == 0);
  IOStatus io_s;

  const Slice log_entry = WriteBatchInternal::Contents(merged_batch);
  const uint64_t log_entry_size = log_entry.size();

  log::Writer* log_writer = logs_.back().writer;
  io_s = log_writer->AddRecord(log_entry);

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
