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

constexpr size_t kMaxSpinThreadCount = 256;
constexpr size_t kMaxSpinCount = 100000;
constexpr uint64_t kMaxSpinTimeMicros = 1000;

void WalSpdb::WritesBatchList::Add(WriteBatch* batch, bool disable_wal,
                                   bool read_lock_for_memtable) {
  if (read_lock_for_memtable) {
    memtable_complete_rwlock_.ReadLock();
  }

  if (!disable_wal) {
    wal_writes_.push_back(batch);
  }

  // Need to set the sequence even if we don't write to WAL because the WAL
  // thread is the one responsible for publishing the last sequence
  const uint64_t sequence = WriteBatchInternal::Sequence(batch);
  if (min_seq_ == 0) {
    min_seq_ = sequence;
  }
  max_seq_ = sequence + WriteBatchInternal::Count(batch) - 1;
}

void WalSpdb::WritesBatchList::MemtableAddComplete() {
  // Batch was added to the memtable, we can release the read lock that is
  // preventing the WAL thread from publishing the last sequence
  memtable_complete_rwlock_.ReadUnlock();
}

void WalSpdb::WritesBatchList::WaitForMemtableWriters() {
  // Successfully acquiring the write lock here means that all of the memtable
  // writers holding the read lock have completed writing into the memtable
  // and released the lock
  WriteLock rl(&memtable_complete_rwlock_);
}

WalSpdb::WalSpdb(DBImpl* db)
    : db_(db), wal_thread_(&WalSpdb::WalWriteThread, this) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  auto thread_handle = wal_thread_.native_handle();
  pthread_setname_np(thread_handle, "speedb:wal");
#endif
#endif
}

WalSpdb::~WalSpdb() {
  Shutdown();
  wal_thread_.join();
}

void WalSpdb::Shutdown() {
  {
    std::unique_lock<std::mutex> lck(wal_thread_mutex_);
    terminate_ = true;
  }
  wal_thread_cv_.notify_one();
}

void* WalSpdb::Add(WriteBatch* batch, bool disable_wal,
                   bool read_lock_for_memtable) {
  // TODO: handle seq_per_batch_ with callbacks?
  const size_t seq_inc = batch->Count();

  void* returned_list = nullptr;

  {
    MutexLock l(&mutex_);
    const uint64_t start_sequence =
        db_->FetchAddLastAllocatedSequence(seq_inc) + 1;
    WriteBatchInternal::SetSequence(batch, start_sequence);

    WritesBatchList& pending_list = GetActiveList();
    if (read_lock_for_memtable) {
      returned_list = &pending_list;
    }

    pending_list.Add(batch, disable_wal, read_lock_for_memtable);
    // No need to trigger work if this isn't the first batch
    if (pending_list.GetMinSeq() < start_sequence) {
      return returned_list;
    }
  }

  {
    std::unique_lock<std::mutex> lck(wal_thread_mutex_);
    pending_buffers_.fetch_add(1, std::memory_order_release);
  }
  wal_thread_cv_.notify_one();
  return returned_list;
}

void WalSpdb::MemtableAddComplete(const void* batch_list) {
  for (WritesBatchList& list : wb_lists_) {
    if (&list == batch_list) {
      list.MemtableAddComplete();
      return;
    }
  }
  // Shouldn't be reached, because a memtable write should finish before
  // the WAL thread released the write batch group
  assert(false);
}

uint64_t WalSpdb::WalWriteComplete() {
  // no need to take the mutex. this is called when we want to write to wal
  WritesBatchList& written_list = GetWrittenList();
  const SequenceNumber written_seq = written_list.GetMaxSeq();

  {
    std::unique_lock<std::mutex> l(notify_wal_write_mutex_);
    last_wal_write_seq_.store(written_seq, std::memory_order_release);
  }
  notify_wal_write_cv_.notify_all();

  written_list.WaitForMemtableWriters();
  written_list.Clear();

  return written_seq;
}

bool WalSpdb::SwitchBatchGroup() {
  MutexLock l(&mutex_);

  if (GetActiveList().IsEmpty()) {
    return false;
  }

  active_buffer_index_ = (active_buffer_index_ + 1) % wb_lists_.size();

  return true;
}

void WalSpdb::WalWriteThread() {
  size_t written_buffers = 0;

  for (;;) {
    const bool terminate = !WaitForPendingWork(written_buffers);

    if (terminate) {
      break;
    }

    if (SwitchBatchGroup()) {
      auto const& immutable_db_options = db_->immutable_db_options();
      StopWatch write_sw(immutable_db_options.clock, immutable_db_options.stats,
                         DB_WAL_WRITE_TIME);

      WritesBatchList& batch_group = GetWrittenList();
      assert(!batch_group.IsEmpty());

      size_t write_with_wal = 0;
      WriteBatch* merged_batch = nullptr;
      const WriteBatch* to_be_cached_state = nullptr;

      // We might have an empty list of WAL writes in case this batch list is
      // memtable only
      if (!batch_group.wal_writes_.empty()) {
        // If we have a single batch which can be fully written into the WAL
        // (i.e. no WAL termination point was set), just run with it and don't
        // do unnecessary copying of data
        if (batch_group.wal_writes_.size() == 1 &&
            batch_group.wal_writes_.front()
                ->GetWalTerminationPoint()
                .is_cleared()) {
          merged_batch = batch_group.wal_writes_.front();
          if (WriteBatchInternal::IsLatestPersistentState(merged_batch)) {
            to_be_cached_state = merged_batch;
          }
          write_with_wal = 1;
        } else {
          merged_batch = &tmp_batch_;
          for (const WriteBatch* batch : batch_group.wal_writes_) {
            Status s = WriteBatchInternal::Append(merged_batch, batch, true);
            // Always returns Status::OK.
            assert(s.ok());
            if (WriteBatchInternal::IsLatestPersistentState(batch)) {
              // We only need to cache the last of such write batch
              to_be_cached_state = batch;
            }
            ++write_with_wal;
          }

          // XXX: This is wrong. Consider the following scenario:
          // A normal write batch arrives, then one with disableWAL, then a
          // normal one again. This results in only two WAL batches, but the
          // sequence number in the middle has already been taken by a memtable
          // only batch, resulting in a wrong sequence number for the data in
          // the WAL.
          WriteBatchInternal::SetSequence(
              merged_batch,
              WriteBatchInternal::Sequence(batch_group.wal_writes_.front()));
        }
      }

      // TBD AYELET - what todo with error??
      db_->SpdbWriteToWAL(merged_batch, write_with_wal, to_be_cached_state);

      if (merged_batch == &tmp_batch_) {
        tmp_batch_.Clear();
      }

      ++written_buffers;
    }

    if (quiesce_cfds_.load()) {
      // i must make sure that the cfds are quiesced before swap
      // must before switch mem table make sure the wal seq was published
      db_->HandleQuiesce();
      {
        std::unique_lock<std::mutex> quiesce_lck(quiesce_mutex_);
        quiesce_cfds_.store(false);
      }
      quiesce_cv_.notify_one();
    }
  }
}

bool WalSpdb::WaitForPendingWork(size_t written_buffers) {
  SystemClock* clock = db_->GetSystemClock();

  // Note: on termination we will spin for min(kMaxSpinTimeMicros,
  // kMaxSpinCount) before we act on the termination request, but that's OK
  // because this only happens on DB closure.
  const uint64_t spin_start = clock->NowMicros();
  for (size_t spin_count = 0; spin_count < kMaxSpinCount; ++spin_count) {
    const size_t pending_buffers =
        pending_buffers_.load(std::memory_order_acquire);
    if (pending_buffers != written_buffers || quiesce_cfds_.load()) {
      return true;
    }

    std::this_thread::yield();

    if (clock->NowMicros() - spin_start >= kMaxSpinTimeMicros) {
      break;
    }
  }

  // Fall back on waiting on the conditional variable
  std::unique_lock<std::mutex> lck(wal_thread_mutex_);
  for (;;) {
    if (terminate_) {
      return false;
    }
    const size_t pending_buffers =
        pending_buffers_.load(std::memory_order_acquire);
    if (pending_buffers != written_buffers || quiesce_cfds_.load()) {
      break;
    }
    wal_thread_cv_.wait(lck);
  }

  return true;
}

Status WalSpdb::WaitForWalWrite(WriteBatch* batch) {
  const uint64_t last_sequence = WriteBatchInternal::Sequence(batch) +
                                 WriteBatchInternal::Count(batch) - 1;

  SystemClock* clock = db_->GetSystemClock();
  Statistics* stats = db_->immutable_db_options().stats;

  StopWatch sw1(clock, stats, DB_WRITE_WAIT_FOR_WAL);

  bool done = false;

  // Don't busy wait if we have too many spinning threads already
  if (++threads_busy_waiting_ <= kMaxSpinThreadCount) {
    const uint64_t spin_start = clock->NowMicros();

    for (size_t spin_count = 0; spin_count < kMaxSpinCount; ++spin_count) {
      if (GetLastWalWriteSeq() >= last_sequence) {
        done = true;
        break;
      }

      std::this_thread::yield();

      if (clock->NowMicros() - spin_start >= kMaxSpinTimeMicros) {
        break;
      }
    }
  }

  --threads_busy_waiting_;

  if (!done) {
    StopWatch sw2(clock, stats, DB_WRITE_WAIT_FOR_WAL_WITH_MUTEX);

    std::unique_lock<std::mutex> wal_wait_lck(notify_wal_write_mutex_);
    while (GetLastWalWriteSeq() < last_sequence) {
      notify_wal_write_cv_.wait(wal_wait_lck);
    }
  }

  // XXX: handle WAL write errors
  return Status::OK();
}

void WalSpdb::Quiesce() {
  {
    std::unique_lock<std::mutex> lck(wal_thread_mutex_);
    quiesce_cfds_.store(true);
  }
  wal_thread_cv_.notify_one();

  std::unique_lock<std::mutex> lck(quiesce_mutex_);
  while (quiesce_cfds_.load()) {
    quiesce_cv_.wait(lck);
  }
}

Status DBImpl::HandleQuiesce() {
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

  if (UNLIKELY(status.ok() && !trim_history_scheduler_.Empty())) {
    status = TrimMemtableHistory(&write_context);
  }

  if (UNLIKELY(status.ok() && !flush_scheduler_.Empty())) {
    status = ScheduleFlushes(&write_context);
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

  if (!trim_history_scheduler_.Empty()) {
    return true;
  }

  if (!flush_scheduler_.Empty()) {
    return true;
  }

  return false;
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

  spdb_write_batch_rwlock_.ReadLock();
  bool write_lock_taken = false;

  if (NeedQuiesce()) {
    spdb_write_batch_rwlock_.ReadUnlock();
    spdb_write_batch_rwlock_.WriteLock();

    write_lock_taken = true;

    // Check again in case another thread already quiesced
    if (NeedQuiesce()) {
      spdb_wal_.Quiesce();
    }
  }

  if (write_options.disableWAL) {
    has_unpersisted_data_.store(true, std::memory_order_relaxed);
  }

  Status status;

  if (!disable_memtable) {
    // TODO: this should be dependant on write group, not just on the write
    // batch
    const bool concurrent_writers = !my_batch->HasMerge();
    const void* list = spdb_wal_.Add(my_batch, write_options.disableWAL,
                                     /* read_lock_for_memtable */ true);
    status = WriteBatchInternal::InsertInto(
        my_batch, column_family_memtables_.get(), &flush_scheduler_,
        &trim_history_scheduler_, write_options.ignore_missing_column_families,
        0 /*recovery_log_number*/, this, concurrent_writers, nullptr, nullptr,
        seq_per_batch_, batch_per_txn_);
    spdb_wal_.MemtableAddComplete(list);
  } else {
    spdb_wal_.Add(my_batch, write_options.disableWAL,
                  /* read_lock_for_memtable */ false);
  }

  if (status.ok()) {
    // Wait for WAL write to complete (this needs to be done even for writes
    // with disableWAL, as the WAL thread is responsible for publishing the
    // last sequence)
    // Both disable_memtable and write_options.disableWAL don't make much sense
    // but since it consumes a sequence number we should wait for it too, as the
    // WAL thread is responsible for publishing the last sequence
    spdb_wal_.WaitForWalWrite(my_batch);
  }

  if (write_lock_taken) {
    spdb_write_batch_rwlock_.WriteUnlock();
  } else {
    spdb_write_batch_rwlock_.ReadUnlock();
  }

  /*if (mutable_db_options_.io_trace) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "(%lu) write completed",
                   env_->GetThreadID());
  }*/

  return status;
}

IOStatus DBImpl::SpdbWriteToWAL(WriteBatch* merged_batch, size_t write_with_wal,
                                const WriteBatch* to_be_cached_state) {
  assert(merged_batch != nullptr || write_with_wal == 0);
  IOStatus io_s;

  if (write_with_wal > 0) {
    const Slice log_entry = WriteBatchInternal::Contents(merged_batch);
    const uint64_t log_size = log_entry.size();

    {
      // TODO: Make sure accessing logs_ with only log writer mutex held is
      // enough and that we don't need to hold onto DB mutex as well
      InstrumentedMutexLock l(&log_write_mutex_);
      log::Writer* log_writer = logs_.back().writer;
      io_s = log_writer->AddRecord(log_entry);
    }

    total_log_size_ += log_entry.size();
    // TODO(myabandeh): it might be unsafe to access alive_log_files_.back()
    // here since alive_log_files_ might be modified concurrently
    alive_log_files_.back().AddSize(log_entry.size());
    log_empty_ = false;

    if (to_be_cached_state != nullptr) {
      cached_recoverable_state_ = *to_be_cached_state;
      cached_recoverable_state_empty_ = false;
    }

    if (io_s.ok()) {
      InternalStats* stats = default_cf_internal_stats_;

      stats->AddDBStats(InternalStats::kIntStatsWalFileBytes, log_size);
      RecordTick(stats_, WAL_FILE_BYTES, log_size);
      stats->AddDBStats(InternalStats::kIntStatsWriteWithWal, write_with_wal);
      RecordTick(stats_, WRITE_WITH_WAL, write_with_wal);
    }
  }

  // XXX: we shouldn't set the last sequence if we had an IO error
  versions_->SetLastSequence(spdb_wal_.WalWriteComplete());

  return io_s;
}

}  // namespace ROCKSDB_NAMESPACE
