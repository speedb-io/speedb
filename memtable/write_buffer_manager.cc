//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <array>
#include <memory>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
WriteBufferManager::WriteBufferManager(
    size_t _buffer_size, std::shared_ptr<Cache> cache, bool allow_stall,
    bool initiate_flushes,
    const FlushInitiationOptions& flush_initiation_options)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_inactive_(0),
      memory_being_freed_(0U),
      cache_res_mgr_(nullptr),
      allow_stall_(allow_stall),
      stall_active_(false),
      initiate_flushes_(initiate_flushes),
      flush_initiation_options_(flush_initiation_options),
      flushes_mu_(new InstrumentedMutex),
      flushes_initiators_mu_(new InstrumentedMutex),
      flushes_wakeup_cv_(new InstrumentedCondVar(flushes_mu_.get())) {
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_res_mgr_ = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kWriteBuffer>>(
        cache, true /* delayed_decrease */);
  }
  if (initiate_flushes_) {
    InitFlushInitiationVars(buffer_size());
  }
}

WriteBufferManager::~WriteBufferManager() {
#ifndef NDEBUG
  std::unique_lock<std::mutex> lock(mu_);
  assert(queue_.empty());
#endif
  TerminateFlushesThread();
}

std::size_t WriteBufferManager::dummy_entries_in_cache_usage() const {
  if (cache_res_mgr_ != nullptr) {
    return cache_res_mgr_->GetTotalReservedCacheSize();
  } else {
    return 0;
  }
}

void WriteBufferManager::ReserveMem(size_t mem) {
  auto is_enabled = enabled();
  size_t new_memory_used = 0U;

  if (cache_res_mgr_ != nullptr) {
    new_memory_used = ReserveMemWithCache(mem);
  } else if (is_enabled) {
    auto old_memory_used =
        memory_used_.fetch_add(mem, std::memory_order_relaxed);
    new_memory_used = old_memory_used + mem;
  }
  if (is_enabled) {
    // Checking outside the locks is not reliable, but avoids locking
    // unnecessarily which is expensive
    if (UNLIKELY(ShouldInitiateAnotherFlushMemOnly(new_memory_used))) {
      ReevaluateNeedForMoreFlushesNoLockHeld(new_memory_used);
    }
  }
}

// Should only be called from write thread
size_t WriteBufferManager::ReserveMemWithCache(size_t mem) {
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache charging fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();

  return new_mem_used;
}

void WriteBufferManager::ScheduleFreeMem(size_t mem) {
  if (enabled()) {
    memory_inactive_.fetch_add(mem, std::memory_order_relaxed);
  }
}

void WriteBufferManager::FreeMemBegin(size_t mem) {
  if (enabled()) {
    memory_being_freed_.fetch_add(mem, std::memory_order_relaxed);
  }
}

// Freeing 'mem' bytes was aborted and that memory is no longer in the process
// of being freed
void WriteBufferManager::FreeMemAborted(size_t mem) {
  if (enabled()) {
    [[maybe_unused]] const auto curr_memory_being_freed =
        memory_being_freed_.fetch_sub(mem, std::memory_order_relaxed);
    assert(curr_memory_being_freed >= mem);
  }
}

void WriteBufferManager::FreeMem(size_t mem) {
  const auto is_enabled = enabled();
  size_t new_memory_used = 0U;

  if (cache_res_mgr_ != nullptr) {
    new_memory_used = FreeMemWithCache(mem);
  } else if (is_enabled) {
    auto old_memory_used =
        memory_used_.fetch_sub(mem, std::memory_order_relaxed);
    assert(old_memory_used >= mem);
    new_memory_used = old_memory_used - mem;
  }

  if (is_enabled) {
    [[maybe_unused]] const auto curr_memory_inactive =
        memory_inactive_.fetch_sub(mem, std::memory_order_relaxed);
    [[maybe_unused]] const auto curr_memory_being_freed =
        memory_being_freed_.fetch_sub(mem, std::memory_order_relaxed);

    assert(curr_memory_inactive >= mem);
    assert(curr_memory_being_freed >= mem);
  }

  // Check if stall is active and can be ended.
  MaybeEndWriteStall();

  if (is_enabled) {
    // Checking outside the locks is not reliable, but avoids locking
    // unnecessarily which is expensive
    if (UNLIKELY(ShouldInitiateAnotherFlushMemOnly(new_memory_used))) {
      ReevaluateNeedForMoreFlushesNoLockHeld(new_memory_used);
    }
  }
}

size_t WriteBufferManager::FreeMemWithCache(size_t mem) {
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  const auto old_mem_used = memory_used_.load(std::memory_order_relaxed);
  assert(old_mem_used >= mem);
  size_t new_mem_used = old_mem_used - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();

  return new_mem_used;
}

void WriteBufferManager::BeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Allocate outside of the lock.
  std::list<StallInterface*> new_node = {wbm_stall};

  {
    std::unique_lock<std::mutex> lock(mu_);
    // Verify if the stall conditions are stil active.
    if (ShouldStall()) {
      stall_active_.store(true, std::memory_order_relaxed);
      queue_.splice(queue_.end(), std::move(new_node));
    }
  }

  // If the node was not consumed, the stall has ended already and we can signal
  // the caller.
  if (!new_node.empty()) {
    new_node.front()->Signal();
  }
}

// Called when memory is freed in FreeMem or the buffer size has changed.
void WriteBufferManager::MaybeEndWriteStall() {
  // Stall conditions have not been resolved.
  if (allow_stall_.load(std::memory_order_relaxed) &&
      IsStallThresholdExceeded()) {
    return;
  }

  // Perform all deallocations outside of the lock.
  std::list<StallInterface*> cleanup;

  std::unique_lock<std::mutex> lock(mu_);
  if (!stall_active_.load(std::memory_order_relaxed)) {
    return;  // Nothing to do.
  }

  // Unblock new writers.
  stall_active_.store(false, std::memory_order_relaxed);

  // Unblock the writers in the queue.
  for (StallInterface* wbm_stall : queue_) {
    wbm_stall->Signal();
  }
  cleanup = std::move(queue_);
}

void WriteBufferManager::RemoveDBFromQueue(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Deallocate the removed nodes outside of the lock.
  std::list<StallInterface*> cleanup;

  if (enabled() && allow_stall_.load(std::memory_order_relaxed)) {
    std::unique_lock<std::mutex> lock(mu_);
    for (auto it = queue_.begin(); it != queue_.end();) {
      auto next = std::next(it);
      if (*it == wbm_stall) {
        cleanup.splice(cleanup.end(), queue_, std::move(it));
      }
      it = next;
    }
  }
  wbm_stall->Signal();
}

std::string WriteBufferManager::GetPrintableOptions() const {
  std::string ret;
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  // The assumed width of the callers display code
  int field_width = 85;

  snprintf(buffer, kBufferSize, "%*s: %" ROCKSDB_PRIszt "\n", field_width,
           "wbm.size", buffer_size());
  ret.append(buffer);

  const Cache* cache = nullptr;
  if (cache_res_mgr_ != nullptr) {
    cache =
        static_cast<CacheReservationManagerImpl<CacheEntryRole::kWriteBuffer>*>(
            cache_res_mgr_.get())
            ->TEST_GetCache();
  }
  snprintf(buffer, kBufferSize, "%*s: %p\n", field_width, "wbm.cache", cache);
  ret.append(buffer);

  snprintf(buffer, kBufferSize, "%*s: %d\n", field_width, "wbm.allow_stalls",
           allow_stall_.load());
  ret.append(buffer);

  snprintf(buffer, kBufferSize, "%*s: %d\n", field_width,
           "wbm.initiate_flushes", IsInitiatingFlushes());
  ret.append(buffer);

  return ret;
}

// ================================================================================================
void WriteBufferManager::RegisterFlushInitiator(
    void* initiator, InitiateFlushRequestCb request) {
  {
    InstrumentedMutexLock lock(flushes_initiators_mu_.get());
    assert(FindInitiator(initiator) == kInvalidInitiatorIdx);

    flush_initiators_.push_back({initiator, request});
    if (flush_initiators_.size() == 1) {
      assert(next_candidate_initiator_idx_ == kInvalidInitiatorIdx);
      next_candidate_initiator_idx_ = 0U;
    }

    assert(next_candidate_initiator_idx_ < flush_initiators_.size());
  }

  // flushes_initiators_mu_ is held but not flushes_mu_
  WakeupFlushInitiationThreadNoLockHeld();
}

void WriteBufferManager::DeregisterFlushInitiator(void* initiator) {
  InstrumentedMutexLock lock(flushes_initiators_mu_.get());
  auto initiator_idx = FindInitiator(initiator);
  assert(IsInitiatorIdxValid(initiator_idx));

  flush_initiators_.erase(flush_initiators_.begin() + initiator_idx);

  // If the deregistered initiator was the next candidate and also the last
  // one, update the next candidate (possibly none left)
  assert(next_candidate_initiator_idx_ != kInvalidInitiatorIdx);
  if (next_candidate_initiator_idx_ >= flush_initiators_.size()) {
    UpdateNextCandidateInitiatorIdx();
  }

  // No need to wake up the flush initiation thread
}

void WriteBufferManager::InitFlushInitiationVars(size_t quota) {
  assert(initiate_flushes_);

  {
    InstrumentedMutexLock lock(flushes_mu_.get());
    additional_flush_step_size_ =
        quota * kStartFlushPercentThreshold / 100 /
        flush_initiation_options_.max_num_parallel_flushes;
    flush_initiation_start_size_ = additional_flush_step_size_;
    min_mutable_flush_size_ = std::min<size_t>(
        quota / (2 * flush_initiation_options_.max_num_parallel_flushes),
        64 * (1 << 20));
    RecalcFlushInitiationSize();
  }

  if (flushes_thread_.joinable() == false) {
    flushes_thread_ =
        std::thread(&WriteBufferManager::InitiateFlushesThread, this);
  }
}

void WriteBufferManager::InitiateFlushesThread() {
  while (true) {
    // Should return true when the waiting should stop (no spurious wakeups
    // guaranteed)
    auto StopWaiting = [this]() {
      return (new_flushes_wakeup_ &&
              (terminate_flushes_thread_ || (num_flushes_to_initiate_ > 0U)));
    };

    InstrumentedMutexLock lock(flushes_mu_.get());
    while (StopWaiting() == false) {
      flushes_wakeup_cv_->Wait();
    }

    new_flushes_wakeup_ = false;

    if (terminate_flushes_thread_) {
      break;
    }

    // The code below tries to initiate num_flushes_to_initiate_ flushes by
    // invoking its registered initiators, and requesting them to initiate a
    // flush of a certain minimum size. The initiation is done in iterations. An
    // iteration is an attempt to give evey initiator an opportunity to flush,
    // in a round-robin ordering. An initiator may or may not be able to
    // initiate a flush. Reasons for not initiating could be:
    // - The flush is less than the specified minimum size.
    // - The initiator is in the process of shutting down or being disposed of.
    //
    // The assumption is that in case flush initiation stopped when
    // num_flushes_to_initiate_ == 0, there will be some future event that will
    // wake up this thread and initiation attempts will be retried:
    // - Initiator will be enabled
    // - A flush in progress will end
    // - The memory_used() will increase above additional_flush_initiation_size_

    // Two iterations:
    // 1. Flushes of a min size.
    // 2. Flushes of any size
    constexpr size_t kNumIters = 2U;
    const std::array<size_t, kNumIters> kMinFlushSizes{min_mutable_flush_size_,
                                                       0U};

    auto iter = 0U;
    while ((iter < kMinFlushSizes.size()) && (num_flushes_to_initiate_ > 0U)) {
      auto num_repeated_failures_to_initiate = 0U;
      while (num_flushes_to_initiate_ > 0U) {
        bool was_flush_initiated = false;
        {
          // Unlocking the flushed_mu_ since flushing (via the initiator cb) may
          // call a WBM service (e.g., ReserveMem()), that, in turn, needs to
          // flushes_mu_lock the same mutex => will get stuck
          InstrumentedMutexUnlock flushes_mu_unlocker(flushes_mu_.get());

          InstrumentedMutexLock initiators_lock(flushes_initiators_mu_.get());
          // Once we are under the flushes_initiators_mu_ lock, we may check:
          // 1. Has the last initiator deregistered?
          // 2. Have all existing initiators failed to initiate a flush?
          if (flush_initiators_.empty() ||
              (num_repeated_failures_to_initiate >= flush_initiators_.size())) {
            break;
          }
          assert(IsInitiatorIdxValid(next_candidate_initiator_idx_));
          auto& initiator = flush_initiators_[next_candidate_initiator_idx_];
          UpdateNextCandidateInitiatorIdx();

          // Assuming initiator would flush (flushes_mu_lock is unlocked and
          // called initiator may call another method that relies on these
          // counters) Not recalculating flush initiation size since the
          // increment & decrement cancel each other with respect to the recalc
          ++num_running_flushes_;
          --num_flushes_to_initiate_;

          // TODO: Use a weak-pointer for the registered initiators. That would
          // allow us to release the flushes_initiators_mu_ mutex before calling
          // the callback (which may take a long time).
          was_flush_initiated = initiator.cb(kMinFlushSizes[iter]);
        }

        if (!was_flush_initiated) {
          // No flush was initiated => undo the counters update
          --num_running_flushes_;
          ++num_flushes_to_initiate_;
          ++num_repeated_failures_to_initiate;
        } else {
          num_repeated_failures_to_initiate = 0U;
        }
      }
      ++iter;
    }
    TEST_SYNC_POINT_CALLBACK(
        "WriteBufferManager::InitiateFlushesThread::DoneInitiationsAttempt",
        &num_flushes_to_initiate_);
  }
}

void WriteBufferManager::TerminateFlushesThread() {
  {
    flushes_mu_->Lock();

    terminate_flushes_thread_ = true;
    WakeupFlushInitiationThreadLockHeld();
  }

  if (flushes_thread_.joinable()) {
    flushes_thread_.join();
  }
}

void WriteBufferManager::FlushStarted(bool wbm_initiated) {
  // num_running_flushes_ is incremented in our thread when initiating flushes
  // => Already accounted for
  if (wbm_initiated || !enabled()) {
    return;
  }

  flushes_mu_->Lock();

  ++num_running_flushes_;
  size_t curr_memory_used = memory_usage();
  RecalcFlushInitiationSize();
  ReevaluateNeedForMoreFlushesLockHeld(curr_memory_used);
}

void WriteBufferManager::FlushEnded(bool /* wbm_initiated */) {
  if (!enabled()) {
    return;
  }

  flushes_mu_->Lock();

  // The WBM may be enabled after a flush has started. In that case
  // the WBM will not be aware of the number of running flushes at the time
  // it is enabled. The counter will become valid once all of the flushes
  // that were running when it was enabled will have completed.
  if (num_running_flushes_ > 0U) {
    --num_running_flushes_;
  }
  size_t curr_memory_used = memory_usage();
  RecalcFlushInitiationSize();
  ReevaluateNeedForMoreFlushesLockHeld(curr_memory_used);
}

void WriteBufferManager::RecalcFlushInitiationSize() {
  flushes_mu_->AssertHeld();

  if (num_running_flushes_ + num_flushes_to_initiate_ >=
      flush_initiation_options_.max_num_parallel_flushes) {
    additional_flush_initiation_size_ = buffer_size();
  } else {
    additional_flush_initiation_size_ =
        flush_initiation_start_size_ +
        additional_flush_step_size_ *
            (num_running_flushes_ + num_flushes_to_initiate_);
  }
}

void WriteBufferManager::ReevaluateNeedForMoreFlushesNoLockHeld(
    size_t curr_memory_used) {
  flushes_mu_->Lock();
  ReevaluateNeedForMoreFlushesLockHeld(curr_memory_used);
}

void WriteBufferManager::ReevaluateNeedForMoreFlushesLockHeld(
    size_t curr_memory_used) {
  assert(enabled());
  flushes_mu_->AssertHeld();

  if (ShouldInitiateAnotherFlush(curr_memory_used)) {
    // need to schedule more
    ++num_flushes_to_initiate_;
    RecalcFlushInitiationSize();
    WakeupFlushInitiationThreadLockHeld();
  } else {
    flushes_mu_->Unlock();
  }
}

uint64_t WriteBufferManager::FindInitiator(void* initiator) const {
  flushes_initiators_mu_->AssertHeld();

  for (auto i = 0U; i < flush_initiators_.size(); ++i) {
    if (flush_initiators_[i].initiator == initiator) {
      return i;
    }
  }

  return kInvalidInitiatorIdx;
}

void WriteBufferManager::WakeupFlushInitiationThreadNoLockHeld() {
  flushes_mu_->Lock();
  WakeupFlushInitiationThreadLockHeld();
}

// Assumed the lock is held
// Releases the lock upon exit
void WriteBufferManager::WakeupFlushInitiationThreadLockHeld() {
  flushes_mu_->AssertHeld();

  new_flushes_wakeup_ = true;

  // Done modifying the shared data. Release the lock so that when the flush
  // initiation thread it may acquire the mutex immediately
  flushes_mu_->Unlock();
  flushes_wakeup_cv_->Signal();
}

void WriteBufferManager::UpdateNextCandidateInitiatorIdx() {
  flushes_initiators_mu_->AssertHeld();

  if (flush_initiators_.empty() == false) {
    if (next_candidate_initiator_idx_ != kInvalidInitiatorIdx) {
      next_candidate_initiator_idx_ =
          ((next_candidate_initiator_idx_ + 1) % flush_initiators_.size());
    } else {
      next_candidate_initiator_idx_ = 0U;
    }
  } else {
    next_candidate_initiator_idx_ = kInvalidInitiatorIdx;
  }
}

bool WriteBufferManager::IsInitiatorIdxValid(uint64_t initiator_idx) const {
  flushes_initiators_mu_->AssertHeld();

  return (initiator_idx < flush_initiators_.size());
}

void WriteBufferManager::TEST_WakeupFlushInitiationThread() {
  WakeupFlushInitiationThreadNoLockHeld();
}

}  // namespace ROCKSDB_NAMESPACE
