
#pragma once
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

class SpdbMemoryManagerClient {
 public:
  SpdbMemoryManagerClient(SpdbMemoryManager *_mem_manager, DBImpl *_db,
                          ColumnFamilyData *_cf)
      : mem_manager_(_mem_manager), db_(_db), cf_(_cf), delay_factor_(0) {
    mem_manager_->RegisterClient(this);
  }

  ~SpdbMemoryManagerClient() { mem_manager_->UnRegisterClient(this); }

  size_t GetMutableDataSize() const {
    return cf()->mem()->ApproximateMemoryUsageFast();
    ;
  }
  size_t GetImMutableDataSize() const {
    return cf()->imm()->ApproximateMemoryUsage();
  }
  size_t GetUnFlushDataSize() const {
    return cf()->imm()->ApproximateUnflushedMemTablesMemoryUsage();
  }

  DBImpl *db() const { return db_; }
  ColumnFamilyData *cf() const { return cf_; }

  void SetDelay(size_t factor) { delay_factor_ = factor; }
  size_t GetDelay() const { return delay_factor_; }

 private:
  SpdbMemoryManager *mem_manager_;
  DBImpl *db_;
  ColumnFamilyData *cf_;
  // delay support
  size_t delay_factor_;
};

class DelayEnforcer {
 public:
  DelayEnforcer(std::shared_ptr<SystemClock> clock, size_t delayed_write_rate,
                size_t max_delay_time_micros);
  void Enforce(size_t byte_size);

  void SetDelayFactor(size_t factor);
  size_t GetDelayFactor() const { return delay_factor_; }
  size_t GetDelayedRate() const {
    return rate_multiplier_ > 0 ? delayed_write_rate_ / rate_multiplier_ : 0;
  }

 private:
  using Nanoseconds = std::chrono::nanoseconds;

  static constexpr Nanoseconds kNanosPerSec =
      std::chrono::duration_cast<Nanoseconds>(std::chrono::seconds(1));

  // 100 microseconds sleep time
  static constexpr Nanoseconds kSleepMin =
      std::chrono::duration_cast<Nanoseconds>(std::chrono::microseconds(100));

  std::shared_ptr<SystemClock> clock_;
  uint64_t delayed_write_rate_;
  uint64_t delay_factor_;
  double rate_multiplier_;
  Nanoseconds max_delay_time_nanos_;
  std::atomic<double> delay_per_byte_nanos_;
  std::atomic<uint64_t> next_request_time_nanos_;
};

}  // namespace ROCKSDB_NAMESPACE
