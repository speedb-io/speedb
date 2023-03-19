//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_controller.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <mutex>
#include <ratio>

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

void WriteController::AddDBToRateMap(uint64_t db_id) {
  std::lock_guard<std::mutex> lock(map_mu_);
  db_id_to_write_rate_map_[db_id] = {};
}

void WriteController::RemoveDBFromRateMap(uint64_t db_id) {
  {
    std::lock_guard<std::mutex> lock(map_mu_);
    if (db_id_to_write_rate_map_.count(db_id)) {
      if (!db_id_to_write_rate_map_[db_id].empty()) {
        total_delayed_.fetch_sub(db_id_to_write_rate_map_[db_id].size());
        set_delayed_write_rate(GetMapMinRate());
      }
      db_id_to_write_rate_map_.erase(db_id);
    }
  }
  MaybeResetCounters();
}

uint64_t WriteController::GetMapMinRate() {
  uint64_t min_rate = max_delayed_write_rate();
  for (const auto& db_id_to_rate : db_id_to_write_rate_map_) {
    for (const auto& cf_id_and_rate : db_id_to_rate.second) {
      if (cf_id_and_rate.second < min_rate) {
        min_rate = cf_id_and_rate.second;
      }
    }
  }
  return min_rate;
}

bool WriteController::IsMinRate(uint32_t cf_id, CfIdToRateMap& cf_map) {
  if (!IsInRateMap(cf_id, cf_map)) {
    return false;
  }
  uint64_t min_rate = delayed_write_rate();
  auto cf_rate = cf_map[cf_id];
  // the cf is already in the map so it shouldnt be possible for it to have a
  // lower rate than the delayed_write_rate_ unless set_max_delayed_write_rate
  // has been used which also sets delayed_write_rate_
  // its fine for several cfs to have the same min_rate.
  return cf_rate <= min_rate;
}

bool WriteController::IsInRateMap(uint32_t cf_id, CfIdToRateMap& cf_map) {
  return cf_map.count(cf_id);
}

void WriteController::HandleNewDelayReq(uint32_t cf_id, uint64_t db_id,
                                        uint64_t cf_write_rate) {
  assert(db_id_to_write_rate_map_.count(db_id));
  CfIdToRateMap& cf_map = db_id_to_write_rate_map_[db_id];
  std::lock_guard<std::mutex> lock(map_mu_);
  bool was_min = IsMinRate(cf_id, cf_map);
  bool inserted = cf_map.insert_or_assign(cf_id, cf_write_rate).second;
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

void WriteController::HandleRemoveDelayReq(uint32_t cf_id, uint64_t db_id) {
  assert(db_id_to_write_rate_map_.count(db_id));
  CfIdToRateMap& cf_map = db_id_to_write_rate_map_[db_id];
  if (IsInRateMap(cf_id, cf_map)) {
    {
      std::lock_guard<std::mutex> lock(map_mu_);
      bool was_min = RemoveDelayReq(cf_id, cf_map);
      if (was_min) {
        set_delayed_write_rate(GetMapMinRate());
      }
    }
    MaybeResetCounters();
  }
}

bool WriteController::RemoveDelayReq(uint32_t cf_id, CfIdToRateMap& cf_map) {
  bool was_min = IsMinRate(cf_id, cf_map);
  [[maybe_unused]] bool erased = cf_map.erase(cf_id);
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

void WriteController::WaitOnCV(const ErrorHandler& error_handler) {
  std::unique_lock<std::mutex> lock(stop_mu_);
  while (error_handler.GetBGError().ok() && IsStopped()) {
    TEST_SYNC_POINT("WriteController::WaitOnCV");
    stop_cv_.wait(lock);
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

// This function is no longer under db mutex since credit_in_bytes_ is atomic
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
