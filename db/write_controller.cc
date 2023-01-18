//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_controller.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <ratio>

#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

std::unique_ptr<WriteControllerToken> WriteController::GetStopToken() {
  ++total_stopped_;
  return std::unique_ptr<WriteControllerToken>(new StopWriteToken(this));
}

std::unique_ptr<WriteControllerToken> WriteController::GetDelayToken(
    uint64_t write_rate) {
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

void WriteController::InsertOrAssignToCfIdAndRateMap(uint32_t cf_id,
                                                     uint64_t write_rate) {
  cf_id_to_write_rate_.insert_or_assign(cf_id,
                                        RateAndWasMin{write_rate, false});
}

uint64_t WriteController::GetMinRate() {
  assert(!cf_id_to_write_rate_.empty());
  uint64_t min_rate = std::numeric_limits<uint64_t>::max();
  uint32_t min_id = 0;
  for (auto& key_val : cf_id_to_write_rate_) {
    key_val.second.is_min = false;
    if (key_val.second.write_rate < min_rate) {
      min_rate = key_val.second.write_rate;
      min_id = key_val.first;
    }
  }
  cf_id_to_write_rate_[min_id].is_min = true;
  assert(min_rate < std::numeric_limits<uint64_t>::max());
  return min_rate;
}

bool WriteController::RemoveCfIdFromRateMap(uint32_t cf_id) {
  assert(cf_id_to_write_rate_.count(cf_id));
  bool was_min = cf_id_to_write_rate_[cf_id].is_min;
  cf_id_to_write_rate_.erase(cf_id);
  return was_min;
}

void WriteController::SetMinRate() { set_delayed_write_rate(GetMinRate()); }

void WriteController::MaybeRemoveSelfAndRefreshDelayRate(uint32_t cf_id) {
  if (cf_id_to_write_rate_.count(cf_id)) {
    bool was_min = RemoveCfIdFromRateMap(cf_id);
    if (was_min && !cf_id_to_write_rate_.empty()) {
      SetMinRate();
    }
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
// This is inside DB mutex, so we can't sleep and need to minimize
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

StopWriteToken::~StopWriteToken() {
  assert(controller_->total_stopped_ >= 1);
  --controller_->total_stopped_;
}

DelayWriteToken::~DelayWriteToken() {
  controller_->total_delayed_--;
  assert(controller_->total_delayed_.load() >= 0);
}

CompactionPressureToken::~CompactionPressureToken() {
  controller_->total_compaction_pressure_--;
  assert(controller_->total_compaction_pressure_ >= 0);
}

}  // namespace ROCKSDB_NAMESPACE
