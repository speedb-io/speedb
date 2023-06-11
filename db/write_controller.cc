//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_controller.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <ratio>

#include "db/error_handler.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

std::unique_ptr<WriteControllerToken> WriteController::GetStopToken() {
  ++total_stopped_;
  return std::unique_ptr<WriteControllerToken>(new StopWriteToken(this));
}

std::unique_ptr<WriteControllerToken> WriteController::GetDelayToken(
    uint64_t write_rate) {
  // this is now only accessed when use_dynamic_delay = false so no need to
  // protect
  assert(is_dynamic_delay() == false);
  if (0 == total_delayed_++) {
    // Starting delay, so reset counters.
    next_refill_time_ = 0;
    credit_in_bytes_ = 0;
  }
  // NOTE: for simplicity, any current credit_in_bytes_ or "debt" in
  // next_refill_time_ will be based on an old rate. This rate will apply
  // for subsequent additional debts and for the next refill.
  set_delayed_write_rate(write_rate);
  return std::unique_ptr<WriteControllerToken>(new DelayWriteToken(this));
}

uint64_t WriteController::TEST_GetMapMinRate() { return GetMapMinRate(); }

uint64_t WriteController::GetMapMinRate() {
  assert(is_dynamic_delay());
  if (!id_to_write_rate_map_.empty()) {
    auto min_elem_iter = std::min_element(
        id_to_write_rate_map_.begin(), id_to_write_rate_map_.end(),
        [](const auto& a, const auto& b) { return a.second < b.second; });
    return std::min(min_elem_iter->second, max_delayed_write_rate());
  } else {
    return max_delayed_write_rate();
  }
}

bool WriteController::IsMinRate(void* client_id) {
  assert(is_dynamic_delay());
  if (!IsInRateMap(client_id)) {
    return false;
  }
  uint64_t min_rate = delayed_write_rate();
  auto cf_rate = id_to_write_rate_map_[client_id];
  // the cf is already in the map so it shouldnt be possible for it to have a
  // lower rate than the delayed_write_rate_ unless set_max_delayed_write_rate
  // has been used which also sets delayed_write_rate_
  // its fine for several cfs to have the same min_rate.
  return cf_rate <= min_rate;
}

bool WriteController::IsInRateMap(void* client_id) {
  return id_to_write_rate_map_.count(client_id);
}

// The usual case is to set the write_rate of this client (cf, write buffer
// manager) only if its lower than the current min (delayed_write_rate_) but
// theres also the case where this client was the min rate (was_min) and now
// its write_rate is higher than the delayed_write_rate_ so we need to find a
// new min from all clients via GetMapMinRate()
void WriteController::HandleNewDelayReq(void* client_id,
                                        uint64_t cf_write_rate) {
  assert(is_dynamic_delay());
  std::lock_guard<std::mutex> lock(map_mu_);
  bool was_min = IsMinRate(client_id);
  bool inserted =
      id_to_write_rate_map_.insert_or_assign(client_id, cf_write_rate).second;
  if (inserted) {
    total_delayed_++;
  }
  uint64_t min_rate = delayed_write_rate();
  if (cf_write_rate <= min_rate) {
    min_rate = cf_write_rate;
  } else if (was_min) {
    min_rate = GetMapMinRate();
  }
  set_delayed_write_rate(min_rate);
}

// Checks if the client is in the id_to_write_rate_map_ , if it is:
// 1. remove it
// 2. decrement total_delayed_
// 3. in case this client had min rate, also set up a new min from the map.
// 4. if total_delayed_ == 0, reset next_refill_time_ and credit_in_bytes_
void WriteController::HandleRemoveDelayReq(void* client_id) {
  assert(is_dynamic_delay());
  {
    std::lock_guard<std::mutex> lock(map_mu_);
    if (!IsInRateMap(client_id)) {
      return;
    }
    bool was_min = RemoveDelayReq(client_id);
    if (was_min) {
      set_delayed_write_rate(GetMapMinRate());
    }
  }
  MaybeResetCounters();
}

bool WriteController::RemoveDelayReq(void* client_id) {
  bool was_min = IsMinRate(client_id);
  [[maybe_unused]] bool erased = id_to_write_rate_map_.erase(client_id);
  assert(erased);
  total_delayed_--;
  return was_min;
}

void WriteController::MaybeResetCounters() {
  std::lock_guard<std::mutex> lock(metrics_mu_);
  if (total_delayed_ == 0) {
    // reset counters.
    next_refill_time_ = 0;
    credit_in_bytes_ = 0;
  }
}

void WriteController::WaitOnCV(std::function<bool()> continue_wait) {
  std::unique_lock<std::mutex> lock(stop_mu_);
  while (continue_wait() && IsStopped()) {
    TEST_SYNC_POINT("WriteController::WaitOnCV");
    // need to time the wait since the stop_cv_ is not signalled if a bg error
    // is raised.
    stop_cv_.wait_for(lock, std::chrono::seconds(1));
  }
}

std::unique_ptr<WriteControllerToken>
WriteController::GetCompactionPressureToken() {
  ++total_compaction_pressure_;
  return std::unique_ptr<WriteControllerToken>(
      new CompactionPressureToken(this));
}

bool WriteController::IsStopped() const {
  return total_stopped_.load(std::memory_order_relaxed) > 0;
}

// This is inside the calling DB mutex, so we can't sleep and need to minimize
// frequency to get time.
// If it turns out to be a performance issue, we can redesign the thread
// synchronization model here.
// The function trust caller will sleep micros returned.
uint64_t WriteController::GetDelay(SystemClock* clock, uint64_t num_bytes) {
  if (total_stopped_.load(std::memory_order_relaxed) > 0) {
    return 0;
  }
  if (total_delayed_.load(std::memory_order_relaxed) == 0) {
    return 0;
  }

  std::lock_guard<std::mutex> lock(metrics_mu_);

  if (credit_in_bytes_ >= num_bytes) {
    credit_in_bytes_ -= num_bytes;
    return 0;
  }
  // The frequency to get time inside DB mutex is less than one per refill
  // interval.
  auto time_now = NowMicrosMonotonic(clock);

  const uint64_t kMicrosPerSecond = 1000000;
  // Refill every 1 ms
  const uint64_t kMicrosPerRefill = 1000;

  if (next_refill_time_ == 0) {
    // Start with an initial allotment of bytes for one interval
    next_refill_time_ = time_now;
  }
  if (next_refill_time_ <= time_now) {
    // Refill based on time interval plus any extra elapsed
    uint64_t elapsed = time_now - next_refill_time_ + kMicrosPerRefill;
    credit_in_bytes_ += static_cast<uint64_t>(
        1.0 * elapsed / kMicrosPerSecond * delayed_write_rate_ + 0.999999);
    next_refill_time_ = time_now + kMicrosPerRefill;

    if (credit_in_bytes_ >= num_bytes) {
      // Avoid delay if possible, to reduce DB mutex release & re-aquire.
      credit_in_bytes_ -= num_bytes;
      return 0;
    }
  }

  // We need to delay to avoid exceeding write rate.
  assert(num_bytes > credit_in_bytes_);
  uint64_t bytes_over_budget = num_bytes - credit_in_bytes_;
  uint64_t needed_delay = static_cast<uint64_t>(
      1.0 * bytes_over_budget / delayed_write_rate_ * kMicrosPerSecond);

  credit_in_bytes_ = 0;
  next_refill_time_ += needed_delay;

  // Minimum delay of refill interval, to reduce DB mutex contention.
  return std::max(next_refill_time_ - time_now, kMicrosPerRefill);
}

uint64_t WriteController::NowMicrosMonotonic(SystemClock* clock) {
  return clock->NowNanos() / std::milli::den;
}

void WriteController::NotifyCV() {
  assert(total_stopped_ >= 1);
  {
    std::lock_guard<std::mutex> lock(stop_mu_);
    --total_stopped_;
  }
  stop_cv_.notify_all();
}

StopWriteToken::~StopWriteToken() { controller_->NotifyCV(); }

DelayWriteToken::~DelayWriteToken() {
  controller_->total_delayed_--;
  assert(controller_->total_delayed_.load() >= 0);
}

CompactionPressureToken::~CompactionPressureToken() {
  controller_->total_compaction_pressure_--;
  assert(controller_->total_compaction_pressure_ >= 0);
}

}  // namespace ROCKSDB_NAMESPACE
