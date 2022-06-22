#include "db/db_impl/external_delay.h"

#include <algorithm>

namespace ROCKSDB_NAMESPACE {

constexpr ExternalDelay::Nanoseconds::rep ExternalDelay::kNanosPerSec;

constexpr ExternalDelay::Nanoseconds ExternalDelay::kSleepMin;
constexpr ExternalDelay::Nanoseconds ExternalDelay::kSleepMax;

void ExternalDelay::Enforce(size_t byte_count) {
  if (delay_per_byte_nanos_ > 0) {
    const auto start_time = Nanoseconds(clock_->NowNanos());
    const auto delay_mul = 1.0 + (1.0 / kNanosPerSec * byte_count);
    const auto current_delay =
        delay_per_byte_nanos_.load(std::memory_order_acquire);
    // We just need the delay per byte to be written atomically, but we don't
    // really care if another thread wins and sets the delay that it calculated.
    delay_per_byte_nanos_.store(current_delay * delay_mul,
                                std::memory_order_release);

    const auto added_delay =
        Nanoseconds(Nanoseconds::rep(byte_count * current_delay));
    const auto request_time =
        added_delay +
        Nanoseconds(Nanoseconds::rep(next_request_time_nanos_.fetch_add(
            added_delay.count(), std::memory_order_relaxed)));
    const auto sleep_time = std::min(request_time - start_time, kSleepMax);
    if (sleep_time.count() > 0 && sleep_time > kSleepMin) {
      const auto sleep_micros =
          std::chrono::duration_cast<std::chrono::microseconds>(
              Nanoseconds(sleep_time))
              .count();
      clock_->SleepForMicroseconds(int(sleep_micros));
    }
  }
}

size_t ExternalDelay::SetDelayWriteRate(size_t new_rate) {
  double old_delay = 0;
  if (new_rate == 0) {
    old_delay = delay_per_byte_nanos_.exchange(0, std::memory_order_release);
  } else {
    next_request_time_nanos_.store(clock_->NowNanos(),
                                   std::memory_order_release);
    old_delay = delay_per_byte_nanos_.exchange(1.0 * kNanosPerSec / new_rate,
                                               std::memory_order_release);
  }
  return old_delay == 0 ? 0 : static_cast<size_t>(kNanosPerSec / old_delay);
}

}  // namespace ROCKSDB_NAMESPACE
