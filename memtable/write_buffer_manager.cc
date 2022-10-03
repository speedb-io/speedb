//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <memory>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache,
                                       bool allow_delays_and_stalls)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_inactive_(0),
      memory_being_freed_(0U),
      cache_res_mgr_(nullptr),
      allow_delays_and_stalls_(allow_delays_and_stalls),
      stall_active_(false) {
#ifndef ROCKSDB_LITE
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_res_mgr_ = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kWriteBuffer>>(
        cache, true /* delayed_decrease */);
  }
#else
  (void)cache;
#endif  // ROCKSDB_LITE
}

WriteBufferManager::~WriteBufferManager() {
#ifndef NDEBUG
  std::unique_lock<std::mutex> lock(mu_);
  assert(queue_.empty());
#endif
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
    UpdateUsageState(new_memory_used, mem, buffer_size());
  }
}

// Should only be called from write thread
size_t WriteBufferManager::ReserveMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  size_t old_mem_used = memory_used_.load(std::memory_order_relaxed);
  size_t new_mem_used = old_mem_used + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache reservation fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();

  return new_mem_used;
#else
  (void)mem;
  return 0U;
#endif  // ROCKSDB_LITE
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

    UpdateUsageState(new_memory_used, -mem, buffer_size());
  }

  // Check if stall is active and can be ended.
  MaybeEndWriteStall();
}

size_t WriteBufferManager::FreeMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
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
#else
  (void)mem;
  return 0U;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::BeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);
  assert(allow_delays_and_stalls_);

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
  // Cannot early-exit on !enabled() because SetBufferSize(0) needs to unblock
  // the writers.
  if (!allow_delays_and_stalls_) {
    return;
  }

  if (IsStallThresholdExceeded()) {
    return;  // Stall conditions have not resolved.
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

  if (enabled() && allow_delays_and_stalls_) {
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

  snprintf(buffer, kBufferSize, "%*s: %d\n", field_width,
           "wbm.allow_delays_and_stalls", IsDelayAllowed());
  ret.append(buffer);

  return ret;
}

namespace {

uint64_t CalcDelayFactor(size_t quota, size_t updated_memory_used,
                         size_t usage_start_delay_threshold) {
  assert(updated_memory_used >= usage_start_delay_threshold);
  double extra_used_memory = updated_memory_used - usage_start_delay_threshold;
  double max_used_memory = quota - usage_start_delay_threshold;

  auto delay_factor =
      (WriteBufferManager::kMaxDelayedWriteFactor * extra_used_memory) /
      max_used_memory;
  if (delay_factor < 1U) {
    delay_factor = 1U;
  }
  return delay_factor;
}

}  // Unnamed Namespace

uint64_t WriteBufferManager::CalcNewCodedUsageState(
    size_t new_memory_used, ssize_t memory_changed_size, size_t quota,
    uint64_t old_coded_usage_state) {
  auto [old_usage_state, old_delay_factor] =
      ParseCodedUsageState(old_coded_usage_state);

  auto new_usage_state = old_usage_state;
  auto new_delay_factor = old_delay_factor;
  auto usage_start_delay_threshold =
      (WriteBufferManager::kStartDelayPercentThreshold * quota) / 100;
  auto change_steps = quota / 100;

  if (new_memory_used < usage_start_delay_threshold) {
    new_usage_state = WriteBufferManager::UsageState::kNone;
  } else if (new_memory_used >= quota) {
    new_usage_state = WriteBufferManager::UsageState::kStop;
  } else {
    new_usage_state = WriteBufferManager::UsageState::kDelay;
  }

  auto calc_new_delay_factor = false;

  if (new_usage_state != old_usage_state) {
    if (new_usage_state == WriteBufferManager::UsageState::kDelay) {
      calc_new_delay_factor = true;
    }
  } else if (new_usage_state == WriteBufferManager::UsageState::kDelay) {
    if (memory_changed_size == 0) {
      calc_new_delay_factor = true;
    } else {
      auto old_memory_used = new_memory_used - memory_changed_size;
      // Calculate & notify only if the change is more than one "step"
      if ((old_memory_used / change_steps) !=
          (new_memory_used / change_steps)) {
        calc_new_delay_factor = true;
      }
    }
  }

  if (calc_new_delay_factor) {
    new_delay_factor =
        CalcDelayFactor(quota, new_memory_used, usage_start_delay_threshold);
  }

  return CalcCodedUsageState(new_usage_state, new_delay_factor);
}

uint64_t WriteBufferManager::CalcCodedUsageState(UsageState usage_state,
                                                 uint64_t delay_factor) {
  switch (usage_state) {
    case UsageState::kNone:
      return kNoneCodedUsageState;
    case UsageState::kDelay:
      assert((delay_factor > kNoneCodedUsageState) &&
             (delay_factor <= kStopCodedUsageState));

      if (delay_factor <= kNoneCodedUsageState) {
        return kNoneCodedUsageState + 1;
      } else if (delay_factor > kStopCodedUsageState) {
        delay_factor = kStopCodedUsageState;
      }
      return delay_factor;
    case UsageState::kStop:
      return kStopCodedUsageState;
    default:
      assert(0);
      // We should never get here (BUG).
      return kNoneCodedUsageState;
  }
}

auto WriteBufferManager::ParseCodedUsageState(uint64_t coded_usage_state)
    -> std::pair<UsageState, uint64_t> {
  if (coded_usage_state <= kNoneCodedUsageState) {
    return {UsageState::kNone, kNoneDelayedWriteFactor};
  } else if (coded_usage_state < kStopCodedUsageState) {
    return {UsageState::kDelay, coded_usage_state};
  } else {
    return {UsageState::kStop, kStopDelayedWriteFactor};
  }
}

void WriteBufferManager::UpdateUsageState(size_t new_memory_used,
                                          ssize_t memory_changed_size,
                                          size_t quota) {
  assert(enabled());
  if (allow_delays_and_stalls_ == false) {
    return;
  }

  auto done = false;
  auto old_coded_usage_state = coded_usage_state_.load();
  auto new_coded_usage_state = old_coded_usage_state;
  while (done == false) {
    new_coded_usage_state = CalcNewCodedUsageState(
        new_memory_used, memory_changed_size, quota, old_coded_usage_state);

    if (old_coded_usage_state != new_coded_usage_state) {
      // Try to update the usage state with the usage state calculated by the
      // current thread. Failure (has_update_succeeded == false) means one or
      // more threads have updated the current state, rendering our own
      // calculation irrelevant. In case has_update_succeeded==false,
      // old_coded_usage_state will be the value of the state that was updated
      // by the other thread(s).
      done = coded_usage_state_.compare_exchange_strong(old_coded_usage_state,
                                                        new_coded_usage_state);

      if (done == false) {
        // Retry. However,
        new_memory_used = memory_usage();
        memory_changed_size = 0U;
      }
    } else {
      done = true;
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
