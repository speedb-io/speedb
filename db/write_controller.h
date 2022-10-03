//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <memory>
#include <numeric>

#include "rocksdb/rate_limiter.h"

namespace ROCKSDB_NAMESPACE {

class SystemClock;
class WriteControllerToken;

// WriteController is controlling write stalls in our write code-path. Write
// stalls happen when compaction can't keep up with write rate.
// All of the methods here (including WriteControllerToken's destructors) need
// to be called while holding DB mutex
class WriteController {
 public:
  enum class DelaySource { kCF = 0, kWBM = 1, kNumSources };
  static constexpr unsigned int DelaySourceValue(DelaySource source) {
    return static_cast<int>(source);
  }
  static constexpr auto kNumDelaySources{
      static_cast<int>(DelaySource::kNumSources)};

 public:
  explicit WriteController(uint64_t _delayed_write_rate = 1024u * 1024u * 32u,
                           int64_t low_pri_rate_bytes_per_sec = 1024 * 1024)
      : total_stopped_(0),
        total_compaction_pressure_(0),
        credit_in_bytes_(0),
        next_refill_time_(0),
        low_pri_rate_limiter_(
            NewGenericRateLimiter(low_pri_rate_bytes_per_sec)) {
    set_max_delayed_write_rate(_delayed_write_rate);
  }
  ~WriteController() = default;

  // When an actor (column family) requests a stop token, all writes will be
  // stopped until the stop token is released (deleted)
  std::unique_ptr<WriteControllerToken> GetStopToken();
  // Delay tokens are managed per delay source. Every delay source controls its
  // own delay indepndently of the other sources. Every call to get a new delay
  // token for a source, sets the delay for that source (and overwrites the
  // previous delay values of that source). The total delay for all writes is
  // the smallest delayed write rate of all the individaul sources (that have
  // delay tokens). Writes to the DB will be controlled under that delayed write
  // rate. Every write needs to call GetDelay() with number of bytes writing to
  // the DB, which returns number of microseconds to sleep.
  //
  // NOTE: Not Thread-Safe
  std::unique_ptr<WriteControllerToken> GetDelayToken(
      DelaySource source, uint64_t delayed_write_rate);
  // When an actor (column family) requests a moderate token, compaction
  // threads will be increased
  std::unique_ptr<WriteControllerToken> GetCompactionPressureToken();

  // these three metods are querying the state of the WriteController
  bool IsStopped() const;
  bool NeedsDelay() const { return (TotalDelayed() > 0); }
  bool NeedsDelay(DelaySource source) const {
    assert(DelaySourceValue(source) < delayed_write_rates_.size());
    return (total_delayed_[DelaySourceValue(source)] > 0);
  }
  bool NeedSpeedupCompaction() const {
    // Compaction depends only on the Column-Families delay source requirements
    return IsStopped() || NeedsDelay(WriteController::DelaySource::kCF) ||
           total_compaction_pressure_ > 0;
  }
  // return how many microseconds the caller needs to sleep after the call
  // num_bytes: how many number of bytes to put into the DB.
  // Prerequisite: DB mutex held.
  uint64_t GetDelay(SystemClock* clock, uint64_t num_bytes);

  // Set the delayed write rate for a specific source.
  // The rate for a source is always the last set value. However, it
  // may affect the global rate only when there are delay tokens for that
  // source. NOTE: Not Thread-Safe
  void set_delayed_write_rate(DelaySource source, uint64_t write_rate) {
    assert(DelaySourceValue(source) < delayed_write_rates_.size());

    // avoid divide 0
    if (write_rate == 0) {
      write_rate = 1u;
    } else if (write_rate > max_delayed_write_rate()) {
      write_rate = max_delayed_write_rate();
    }

    delayed_write_rates_[DelaySourceValue(source)] = write_rate;
    UpdateDelayedWriteRate();
  }

  // NOTE: Not Thread-Safe
  void set_max_delayed_write_rate(uint64_t write_rate) {
    // avoid divide 0
    if (write_rate == 0) {
      write_rate = 1u;
    }

    max_delayed_write_rate_ = write_rate;
    for (auto source_value = 0U; source_value < total_delayed_.size();
         ++source_value) {
      if (total_delayed_[source_value] == 0U) {
        delayed_write_rates_[source_value] = max_delayed_write_rate_;
      }
    }
    // update delayed_write_rate_ as well
    UpdateDelayedWriteRate();
  }

  // Get the global delay write rate (not specific to any source)
  // NOTE: Only sources with delay tokens affect the global rate
  uint64_t delayed_write_rate() const { return delayed_write_rate_; }

  // Returns the last set value for source
  uint64_t delayed_write_rate(DelaySource source) const {
    assert(DelaySourceValue(source) < delayed_write_rates_.size());
    return delayed_write_rates_[DelaySourceValue(source)];
  }

  uint64_t max_delayed_write_rate() const { return max_delayed_write_rate_; }

  RateLimiter* low_pri_rate_limiter() { return low_pri_rate_limiter_.get(); }

  uint64_t TEST_delayed_write_rate(DelaySource source) const {
    return delayed_write_rates_[DelaySourceValue(source)];
  }

 private:
  uint64_t NowMicrosMonotonic(SystemClock* clock);
  int TotalDelayed() const {
    return std::accumulate(total_delayed_.begin(), total_delayed_.end(), 0);
  }

  void UpdateDelayedWriteRate() {
    // The effective global delayed write rate is the lowest (minimal)
    // write rate of all delays set by the sources (with delay tokens)
    delayed_write_rate_ = max_delayed_write_rate_;
    for (auto delay_source_value = 0; delay_source_value < kNumDelaySources;
         ++delay_source_value) {
      if ((delayed_write_rates_[delay_source_value] < delayed_write_rate_) &&
          NeedsDelay(DelaySource(delay_source_value))) {
        delayed_write_rate_ = delayed_write_rates_[delay_source_value];
      }
    }
  }

 private:
  friend class WriteControllerToken;
  friend class StopWriteToken;
  friend class DelayWriteToken;
  friend class CompactionPressureToken;

  std::atomic<int> total_stopped_;
  std::array<std::atomic<int>, kNumDelaySources> total_delayed_{};
  std::atomic<int> total_compaction_pressure_;

  // Number of bytes allowed to write without delay
  uint64_t credit_in_bytes_ = 0U;
  // Next time that we can add more credit of bytes
  uint64_t next_refill_time_ = 0U;
  // Write rate set when initialization or by `DBImpl::SetDBOptions`
  uint64_t max_delayed_write_rate_ = 0U;
  // Current write rate (bytes / second)
  std::array<uint64_t, kNumDelaySources> delayed_write_rates_{};
  uint64_t delayed_write_rate_ = 0U;

  std::unique_ptr<RateLimiter> low_pri_rate_limiter_;
};

class WriteControllerToken {
 public:
  explicit WriteControllerToken(WriteController* controller)
      : controller_(controller) {}
  virtual ~WriteControllerToken() {}

 protected:
  WriteController* controller_;

 private:
  // no copying allowed
  WriteControllerToken(const WriteControllerToken&) = delete;
  void operator=(const WriteControllerToken&) = delete;
};

class StopWriteToken : public WriteControllerToken {
 public:
  explicit StopWriteToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~StopWriteToken();
};

class DelayWriteToken : public WriteControllerToken {
 public:
  explicit DelayWriteToken(WriteController* controller,
                           WriteController::DelaySource source)
      : WriteControllerToken(controller), source_(source) {}
  virtual ~DelayWriteToken();

 private:
  WriteController::DelaySource source_;
};

class CompactionPressureToken : public WriteControllerToken {
 public:
  explicit CompactionPressureToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~CompactionPressureToken();
};

}  // namespace ROCKSDB_NAMESPACE
