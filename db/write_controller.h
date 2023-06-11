//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "rocksdb/rate_limiter.h"

namespace ROCKSDB_NAMESPACE {

class SystemClock;
class WriteControllerToken;
class ErrorHandler;
// WriteController is controlling write stalls in our write code-path. Write
// stalls happen when compaction can't keep up with write rate.
// All of the methods here (including WriteControllerToken's destructors) need
// to be called while holding DB mutex when dynamic_delay_ is false.
// use_dynamic_delay is the options flag (in include/rocksdb/options.h) which
// is passed to the ctor of WriteController for setting dynamic_delay_.
// when dynamic_delay_ is true, then the WriteController can be shared across
// many dbs which requires using metrics_mu_ and map_mu_.
// In a shared state (global delay mechanism), the WriteController can also
// receive delay requirements from the WriteBufferManager.
class WriteController {
 public:
  explicit WriteController(bool dynamic_delay,
                           uint64_t _delayed_write_rate = 1024u * 1024u * 16u,
                           int64_t low_pri_rate_bytes_per_sec = 1024 * 1024)
      : dynamic_delay_(dynamic_delay),
        total_stopped_(0),
        total_delayed_(0),
        total_compaction_pressure_(0),
        credit_in_bytes_(0),
        next_refill_time_(0),
        low_pri_rate_limiter_(
            NewGenericRateLimiter(low_pri_rate_bytes_per_sec)) {
    set_max_delayed_write_rate(_delayed_write_rate);
  }
  ~WriteController() = default;

  static constexpr uint64_t kMinWriteRate =
      16 * 1024u;  // Minimum write rate 16KB/s.

  // When an actor (column family) requests a stop token, all writes will be
  // stopped until the stop token is released (deleted)
  std::unique_ptr<WriteControllerToken> GetStopToken();
  // When an actor (column family) requests a delay token, total delay for all
  // writes to the DB will be controlled under the delayed write rate. Every
  // write needs to call GetDelay() with number of bytes writing to the DB,
  // which returns number of microseconds to sleep.
  std::unique_ptr<WriteControllerToken> GetDelayToken(
      uint64_t delayed_write_rate);
  // When an actor (column family) requests a moderate token, compaction
  // threads will be increased
  std::unique_ptr<WriteControllerToken> GetCompactionPressureToken();

  // these three metods are querying the state of the WriteController
  bool IsStopped() const;
  bool NeedsDelay() const { return total_delayed_.load() > 0; }
  bool NeedSpeedupCompaction() const {
    return IsStopped() || NeedsDelay() || total_compaction_pressure_.load() > 0;
  }
  // return how many microseconds the caller needs to sleep after the call
  // num_bytes: how many number of bytes to put into the DB.
  // Prerequisite: DB mutex held.
  uint64_t GetDelay(SystemClock* clock, uint64_t num_bytes);
  void set_delayed_write_rate(uint64_t write_rate) {
    std::lock_guard<std::mutex> lock(metrics_mu_);
    // avoid divide 0
    if (write_rate == 0) {
      write_rate = 1u;
    } else if (write_rate > max_delayed_write_rate()) {
      write_rate = max_delayed_write_rate();
    }
    delayed_write_rate_ = write_rate;
  }

  void set_max_delayed_write_rate(uint64_t write_rate) {
    std::lock_guard<std::mutex> lock(metrics_mu_);
    // avoid divide 0
    if (write_rate == 0) {
      write_rate = 1u;
    }
    max_delayed_write_rate_ = write_rate;
    // update delayed_write_rate_ as well
    delayed_write_rate_ = write_rate;
  }

  uint64_t delayed_write_rate() const { return delayed_write_rate_; }

  uint64_t max_delayed_write_rate() const { return max_delayed_write_rate_; }

  RateLimiter* low_pri_rate_limiter() { return low_pri_rate_limiter_.get(); }

  bool is_dynamic_delay() const { return dynamic_delay_; }

  int TEST_total_delayed_count() const { return total_delayed_.load(); }

  /////// methods and members used when dynamic_delay_ == true. ///////
  // For now, clients can be column families or WriteBufferManagers
  // and the Id (void*) is simply the pointer to their obj
  using ClientIdToRateMap = std::unordered_map<void*, uint64_t>;

  void HandleNewDelayReq(void* client_id, uint64_t cf_write_rate);

  // Removes a client's delay and updates the Write Controller's effective
  // delayed write rate if applicable
  void HandleRemoveDelayReq(void* client_id);

  uint64_t TEST_GetMapMinRate();

  void WaitOnCV(std::function<bool()> continue_wait);
  void NotifyCV();

 private:
  bool IsMinRate(void* client_id);
  bool IsInRateMap(void* client_id);
  // REQUIRES: cf_id is in the rate map.
  // returns if the element removed had rate == delayed_write_rate_
  bool RemoveDelayReq(void* client_id);
  void MaybeResetCounters();

  // returns the min rate from id_to_write_rate_map_
  // REQUIRES: write_controller map_mu_ mutex held.
  uint64_t GetMapMinRate();

  // Whether Speedb's dynamic delay is used
  bool dynamic_delay_ = true;

  std::mutex map_mu_;
  ClientIdToRateMap id_to_write_rate_map_;

  // The mutex used by stop_cv_
  std::mutex stop_mu_;
  std::condition_variable stop_cv_;
  /////// end of methods and members used when dynamic_delay_ == true. ///////

  uint64_t NowMicrosMonotonic(SystemClock* clock);

  friend class WriteControllerToken;
  friend class StopWriteToken;
  friend class DelayWriteToken;
  friend class CompactionPressureToken;

  std::atomic<int> total_stopped_;
  std::atomic<int> total_delayed_;
  std::atomic<int> total_compaction_pressure_;

  // mutex to protect below 4 members which is required when WriteController is
  // shared across several dbs.
  std::mutex metrics_mu_;
  // Number of bytes allowed to write without delay
  std::atomic<uint64_t> credit_in_bytes_;
  // Next time that we can add more credit of bytes
  std::atomic<uint64_t> next_refill_time_;
  // Write rate set when initialization or by `DBImpl::SetDBOptions`
  std::atomic<uint64_t> max_delayed_write_rate_;
  // Current write rate (bytes / second)
  std::atomic<uint64_t> delayed_write_rate_;

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
  explicit DelayWriteToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~DelayWriteToken();
};

class CompactionPressureToken : public WriteControllerToken {
 public:
  explicit CompactionPressureToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~CompactionPressureToken();
};

}  // namespace ROCKSDB_NAMESPACE
