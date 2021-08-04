//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl/db_spdb_impl_write.h"

#include "db/db_impl/db_impl.h"
#include "db/write_batch_internal.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "util/mutexlock.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

static constexpr size_t kQuiesceCheckTimeoutInSecs = 5;

void SpdbWriteImpl::WritesBatchList::Add(WriteBatch* batch, bool disable_wal,
                                         bool disable_memtable,
                                         bool* first_batch) {
  const size_t seq_inc = batch->Count();

  max_seq_ = WriteBatchInternal::Sequence(batch) + seq_inc - 1;

  if (batch->HasMerge()) {
    // need to wait all prev batches completed to write to memetable and avoid
    // new batches to write to memetable before this one
    WriteLock wl(&memtable_ref_rwlock_);
  }
  if (!disable_wal) {
    wal_writes_.push_back(batch);
  }
  if (wal_writes_.size() == 1) {
    // first wal batch . should take the buffer_write_rw_lock_ as write
    *first_batch = true;
    buffer_write_rw_lock_.WriteLock();
  } else {
    if (!disable_memtable) {
      memtable_ref_rwlock_.ReadLock();
    }
  }
}

void SpdbWriteImpl::WritesBatchList::WriteBatchComplete(bool first_batch,
                                                        bool disable_wal,
                                                        bool disable_memtable) {
  if (first_batch) {
    buffer_write_rw_lock_.WriteUnlock();
    WriteLock wl(&memtable_ref_rwlock_);
  } else {
    if (!disable_memtable) {
      // Batch was added to the memtable, we can release the memtable_ref.
      memtable_ref_rwlock_.ReadUnlock();
    }
    if (!disable_wal) {
      ReadLock rl(&buffer_write_rw_lock_);
    }
  }
}

void SpdbWriteImpl::WriteBatchComplete(void* list, bool first_batch,
                                       bool disable_wal,
                                       bool disable_memtable) {
  WritesBatchList* write_batch_list = static_cast<WritesBatchList*>(list);
  if (first_batch) {
    SwitchAndWriteBatchGroup();
  } else {
    write_batch_list->WriteBatchComplete(first_batch, disable_wal,
                                         disable_memtable);
  }
}

void SpdbWriteImpl::SpdbQuiesceWriteThread() {
  for (;;) {
    if (quiesce_thread_terminate_) {
      break;
    }
    std::unique_lock<std::mutex> lck(quiesce_thread_mutex_);
    while (quiesce_thread_cv_.wait_for(
               lck, std::chrono::seconds(kQuiesceCheckTimeoutInSecs)) ==
           std::cv_status::timeout) {
      if (quiesce_needed_.load() == false) {
        if (db_->NeedQuiesce()) {
          quiesce_needed_.store(true);
        }
      }
    }
  }
}

SpdbWriteImpl::SpdbWriteImpl(DBImpl* db)
    : db_(db), quiesce_thread_(&SpdbWriteImpl::SpdbQuiesceWriteThread, this) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  auto thread_handle = quiesce_thread_.native_handle();
  pthread_setname_np(thread_handle, "speedb:quiesce");
#endif
#endif
}

SpdbWriteImpl::~SpdbWriteImpl() {
  Shutdown();
  quiesce_thread_.join();
}

void SpdbWriteImpl::Shutdown() {
  {
    std::unique_lock<std::mutex> lck(quiesce_thread_mutex_);
    quiesce_thread_terminate_ = true;
  }
  quiesce_thread_cv_.notify_one();
}

Status DBImpl::Quiesce() {
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

bool DBImpl::NeedQuiesce() {
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

void* SpdbWriteImpl::Add(WriteBatch* batch, bool disable_wal,
                         bool disable_memtable, bool* first_batch) {
  WritesBatchList& pending_list = GetActiveList();
  pending_list.Add(batch, disable_wal, disable_memtable, first_batch);
  return &pending_list;
}

void SpdbWriteImpl::Lock() { add_buffer_mutex_.Lock(); }

void SpdbWriteImpl::Unlock() { add_buffer_mutex_.Unlock(); }

SpdbWriteImpl::WritesBatchList& SpdbWriteImpl::SwitchBatchGroup() {
  MutexLock l(&add_buffer_mutex_);
  WritesBatchList& batch_group = wb_lists_[active_buffer_index_];
  active_buffer_index_ = (active_buffer_index_ + 1) % wb_lists_.size();
  if (quiesce_needed_.load() == true) {
    { WriteLock bg_wl(&batch_group.memtable_ref_rwlock_); }
    Status status = db_->Quiesce();
    quiesce_needed_.store(false);
  }
  return batch_group;
}

void SpdbWriteImpl::SwitchAndWriteBatchGroup() {
  // take the wal write rw lock
  WriteLock wl(&wal_write_rwlock_);
  WritesBatchList& batch_group = SwitchBatchGroup();
  if (!batch_group.wal_writes_.empty()) {
    auto const& immutable_db_options = db_->immutable_db_options();
    StopWatch write_sw(immutable_db_options.clock, immutable_db_options.stats,
                       DB_WAL_WRITE_TIME);

    size_t write_with_wal = 0;
    WriteBatch* merged_batch = nullptr;
    const WriteBatch* to_be_cached_state = nullptr;
    uint64_t wal_write_batch_seq = 0;

    if (batch_group.wal_writes_.size() == 1 && batch_group.wal_writes_.front()
                                                   ->GetWalTerminationPoint()
                                                   .is_cleared()) {
      merged_batch = batch_group.wal_writes_.front();
      if (WriteBatchInternal::IsLatestPersistentState(merged_batch)) {
        to_be_cached_state = merged_batch;
      }
      write_with_wal = 1;
      wal_write_batch_seq = WriteBatchInternal::Sequence(merged_batch);
    } else {
      uint64_t progress_batch_seq;
      merged_batch = &tmp_batch_;
      for (const WriteBatch* batch : batch_group.wal_writes_) {
        if (write_with_wal != 0 &&
            (progress_batch_seq != WriteBatchInternal::Sequence(batch))) {
          // this can happened if we have a batch group that consists no wal
          // writes...
          WriteBatchInternal::SetSequence(merged_batch, wal_write_batch_seq);
          // TBD AYELET - what todo with error??
          IOStatus io_status = db_->SpdbWriteToWAL(merged_batch, write_with_wal,
                                                   to_be_cached_state);
          // reset counter and state
          tmp_batch_.Clear();
          write_with_wal = 0;
          to_be_cached_state = nullptr;
        }
        if (write_with_wal == 0) {
          wal_write_batch_seq = WriteBatchInternal::Sequence(batch);
        }
        progress_batch_seq =
            WriteBatchInternal::Sequence(batch) + batch->Count();
        Status s = WriteBatchInternal::Append(merged_batch, batch, true);
        // Always returns Status::OK.()
        assert(s.ok());
        if (WriteBatchInternal::IsLatestPersistentState(batch)) {
          // We only need to cache the last of such write batch
          to_be_cached_state = batch;
        }
        ++write_with_wal;
      }

      WriteBatchInternal::SetSequence(merged_batch, wal_write_batch_seq);
    }
    IOStatus io_status =
        db_->SpdbWriteToWAL(merged_batch, write_with_wal, to_be_cached_state);

    if (merged_batch == &tmp_batch_) {
      tmp_batch_.Clear();
    }
  }
  batch_group.WriteBatchComplete(true);

  db_->SetLastSequence(batch_group.GetMaxSeq());
  batch_group.Clear();
}

Status DBImpl::SpdbWrite(const WriteOptions& write_options,
                         WriteBatch* my_batch, bool disable_memtable) {
  assert(my_batch != nullptr && WriteBatchInternal::Count(my_batch) > 0);
  StopWatch write_sw(immutable_db_options_.clock, immutable_db_options_.stats,
                     DB_WRITE);

  if (error_handler_.IsDBStopped()) {
    return error_handler_.GetBGError();
  }

  last_batch_group_size_ = WriteBatchInternal::ByteSize(my_batch);

  spdb_write_.GetQuiesceRWLock().ReadLock();

  if (write_options.disableWAL) {
    has_unpersisted_data_.store(true, std::memory_order_relaxed);
  }

  Status status;
  bool first_batch = false;
  void* list;
  spdb_write_.Lock();
  const uint64_t sequence =
      versions_->FetchAddLastAllocatedSequence(my_batch->Count()) + 1;
  WriteBatchInternal::SetSequence(my_batch, sequence);

  list = spdb_write_.Add(my_batch, write_options.disableWAL, disable_memtable,
                         &first_batch);
  if (!my_batch->HasMerge()) {
    spdb_write_.Unlock();
  }
  if (!disable_memtable) {
    status = WriteBatchInternal::InsertInto(
        my_batch, column_family_memtables_.get(), &flush_scheduler_,
        &trim_history_scheduler_, write_options.ignore_missing_column_families,
        0 /*recovery_log_number*/, this, !my_batch->HasMerge(), nullptr,
        nullptr, seq_per_batch_, batch_per_txn_);
    if (my_batch->HasMerge()) {
      spdb_write_.Unlock();
    }
  }

  // handle !status.ok()
  spdb_write_.WriteBatchComplete(list, first_batch, write_options.disableWAL,
                                 disable_memtable);

  spdb_write_.GetQuiesceRWLock().ReadUnlock();

  if (io_tracer_) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "(%lu) write completed",
                   env_->GetThreadID());
  }

  return status;
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
