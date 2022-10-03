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
    DelaySource source, uint64_t write_rate) {
  if (TotalDelayed() == 0) {
    // Starting delay, so reset counters.
    next_refill_time_ = 0;
    credit_in_bytes_ = 0;
  }

  ++total_delayed_[DelaySourceValue(source)];

  // NOTE: for simplicity, any current credit_in_bytes_ or "debt" in
  // next_refill_time_ will be based on an old rate. This rate will apply
  // for subsequent additional debts and for the next refill.
  set_delayed_write_rate(source, write_rate);
  return std::unique_ptr<WriteControllerToken>(
      new DelayWriteToken(this, source));
}

std::unique_ptr<WriteControllerToken>
WriteController::GetCompactionPressureToken() {
  ++total_compaction_pressure_;
  return std::unique_ptr<WriteControllerToken>(
      new CompactionPressureToken(this));
}

bool WriteController::IsStopped() const {
  // TODO - All other uses of the token counters are using the default memory
  // order, not relaxed!!!
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
  if (TotalDelayed() == 0) {
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
  auto total_delayed_before =
      controller_->total_delayed_[WriteController::DelaySourceValue(source_)]--;
  assert(total_delayed_before > 0);

  if (total_delayed_before == 1) {
    // When all tokens of a source are cleared, it should not lower the global
    // max delayed write rate
    controller_->set_delayed_write_rate(source_,
                                        controller_->max_delayed_write_rate());
  }
}

CompactionPressureToken::~CompactionPressureToken() {
  controller_->total_compaction_pressure_--;
  assert(controller_->total_compaction_pressure_ >= 0);
}

}  // namespace ROCKSDB_NAMESPACE
