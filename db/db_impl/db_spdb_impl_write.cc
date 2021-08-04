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

#include <cinttypes>

#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/write_batch_internal.h"
#include "logging/logging.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/perf_context_imp.h"
#include "options/options_helper.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// add_buffer_mutex_ is held
void SpdbWriteImpl::WritesBatchList::Add(WriteBatch* batch,
                                         const WriteOptions& write_options,
                                         bool* leader_batch) {
  const size_t seq_inc = batch->Count();
  max_seq_ = WriteBatchInternal::Sequence(batch) + seq_inc - 1;

  if (!write_options.disableWAL) {
    wal_writes_.push_back(batch);
  } else {
    memtable_only_writes_.push_back(batch);
  }
  if (write_options.sync) {
    need_sync_ = true;
  }
  if (empty_) {
    // first wal batch . should take the buffer_write_rw_lock_ as write
    *leader_batch = true;
    buffer_write_rw_lock_.WriteLock();
    empty_ = false;
  }
  write_ref_rwlock_.ReadLock();
}

void DBImpl::RollbackBatch(WriteBatch* batch) {
  WriteBatchInternal::MarkDelete(batch);
  Status status = WriteBatchInternal::InsertInto(
      batch, column_family_memtables_.get(), &flush_scheduler_,
      &trim_history_scheduler_, true, 0 /*recovery_log_number*/, this, true,
      nullptr, nullptr, seq_per_batch_, batch_per_txn_);
  if (!status.ok()) {
    assert(false);
  }
}

void SpdbWriteImpl::WritesBatchList::WriteBatchLeaderComplete(
    DBImpl* db, uint64_t* seq_used, Status s) {
  // Batch was added to the memtable, we can release the memtable_ref.
  write_ref_rwlock_.ReadUnlock();
  {
    // make sure all batches wrote to memtable (if needed) to be able progress
    // the version
    WriteLock wl(&write_ref_rwlock_);
  }
  if (!s.ok()) {
    // need all group batches to set as  deleted
    for (WriteBatch* batch : wal_writes_) {
      db->RollbackBatch(batch);
    }
  }
  db->SetLastSequence(max_seq_);
  if (seq_used) {
    *seq_used = max_seq_;
  }
  // wal write has been completed wal waiters will be released
  buffer_write_rw_lock_.WriteUnlock();
}

void SpdbWriteImpl::WritesBatchList::WriteBatchComplete() {
  // Batch was added to the memtable, we can release the memtable_ref.
  write_ref_rwlock_.ReadUnlock();
  // wait wal write completed
  ReadLock rl(&buffer_write_rw_lock_);
}

void SpdbWriteImpl::WritesBatchList::WaitForPendingWrites() {
  // make sure all batches wrote to memtable (ifneeded) to be able progress the
  // version
  WriteLock wl(&write_ref_rwlock_);
}

Status SpdbWriteImpl::WriteBatchLeaderComplete(uint64_t* seq_used) {
  Status status = SwitchAndWriteBatchGroup(seq_used);
  return status;
}

void SpdbWriteImpl::WriteBatchComplete(void* list) {
  Status status;
  WritesBatchList* write_batch_list = static_cast<WritesBatchList*>(list);
  write_batch_list->WriteBatchComplete();
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

// REQUIRES: mutex_ is held
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

void* SpdbWriteImpl::AddMerge(WriteBatch* batch,
                              const WriteOptions& write_options,
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
  pending_list.Add(batch, write_options, leader_batch);
  return &pending_list;
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

SpdbWriteImpl::WritesBatchList* SpdbWriteImpl::SwitchBatchGroup() {
  MutexLock l(&add_buffer_mutex_);
  WritesBatchList* batch_group = &wb_lists_[active_buffer_index_];
  active_buffer_index_ = (active_buffer_index_ + 1) % wb_lists_.size();
  return batch_group;
}

Status SpdbWriteImpl::SwitchAndWriteBatchGroup(uint64_t* seq_used) {
  // take the wal write rw lock from protecting another batch group wal write
  Status s;
  WritesBatchList* batch_group = nullptr;
  wal_write_mutex_.Lock();
  batch_group = SwitchBatchGroup();

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
      io_s = db_->SpdbWriteToWAL(wal_batch, 1, to_be_cached_state);
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
        s = WriteBatchInternal::Append(merged_batch, batch, true);
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
      ROCKS_LOG_ERROR(db_->immutable_db_options().info_log,
                      "Error write to wal!!! %s", io_s.ToString().c_str());
      s = io_s;
    } else {
      if (batch_group->need_sync_) {
        io_s = db_->SpdbSyncWAL();
        if (!io_s.ok()) {
          ROCKS_LOG_ERROR(db_->immutable_db_options().info_log,
                          "Error sync to wal!!! %s", io_s.ToString().c_str());
          s = io_s;
        }
      }
    }
  }

  batch_group->WriteBatchLeaderComplete(db_, seq_used, s);
  batch_group->Clear();
  wal_write_mutex_.Unlock();
  return s;
}
Status DBImpl::SpdbWrite(const WriteOptions& write_options, WriteBatch* batch,
                         WriteCallback* callback, uint64_t* log_used,
                         bool disable_memtable, uint64_t* seq_used) {
  assert(batch != nullptr);
  StopWatch write_sw(immutable_db_options_.clock, immutable_db_options_.stats,
                     DB_WRITE);
  if (error_handler_.IsDBStopped()) {
    return error_handler_.GetBGError();
  }

  Status status;
  if (callback) {
    status = callback->Callback(this);
    if (!status.ok()) {
      return status;
    }
  }

  if (WriteBatchInternal::Count(batch) > 0) {
    last_batch_group_size_ = WriteBatchInternal::ByteSize(batch);
  }
  spdb_write_->Lock(true);

  if (write_options.disableWAL) {
    has_unpersisted_data_.store(true, std::memory_order_relaxed);
  }

  bool leader_batch = false;
  void* list;
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
  if (leader_batch) {
    // handle !status.ok() inside
    status = spdb_write_->WriteBatchLeaderComplete(seq_used);
    if (log_used) {
      *log_used = logfile_number_;
    }
  } else {
    spdb_write_->WriteBatchComplete(list);
  }

  spdb_write_->Unlock(true);
  if (leader_batch) {
    if (CheckIfActionNeeded()) {
      spdb_write_->Lock(false);
      RegisterFlushOrTrim();
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

IOStatus DBImpl::SpdbSyncWAL() {
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

  return io_s;
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
