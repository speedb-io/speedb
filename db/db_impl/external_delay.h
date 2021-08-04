#pragma once

#include <atomic>
#include <chrono>
#include <memory>

#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

class ExternalDelay {
 public:
  ExternalDelay(std::shared_ptr<SystemClock> clock)
      : clock_(std::move(clock)), delay_per_byte_nanos_(0) {}

  void Enforce(size_t byte_count);

  bool Reset() { return SetDelayWriteRate(0) != 0; }

  size_t SetDelayWriteRate(size_t rate);

 private:
  using Nanoseconds = std::chrono::nanoseconds;

  static constexpr Nanoseconds::rep kNanosPerSec =
      std::chrono::duration_cast<Nanoseconds>(std::chrono::seconds(1)).count();

  static constexpr Nanoseconds kSleepMin =
      std::chrono::duration_cast<Nanoseconds>(std::chrono::microseconds(100));
  static constexpr Nanoseconds kSleepMax =
      std::chrono::duration_cast<Nanoseconds>(std::chrono::seconds(1));

  std::shared_ptr<SystemClock> clock_;
  std::atomic<double> delay_per_byte_nanos_;
  std::atomic<uint64_t> next_request_time_nanos_;
};

}  // namespace ROCKSDB_NAMESPACE
