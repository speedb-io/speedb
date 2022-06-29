#include "memory_manager.h"

#include <math.h>
#include <rocksdb/options.h>
#include <rocksdb/write_buffer_manager.h>

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

SpdbMemoryManager::SpdbMemoryManager(const SpdbMemoryManagerOptions &options)
    : WriteBufferManager(options.dirty_data_size),
      cache_(options.cache),
      n_parallel_flushes_(options.n_parallel_flushes),
      n_running_flushes_(0),
      n_scheduled_flushes_(0),
      size_pendding_for_flush_(0),
      next_recalc_size_(0),
      memsize_delay_factor_(0),
      terminated_(false) {
  delay_enforcer_ = new DelayEnforcer(options.clock, options.delayed_write_rate,
                                      options.max_delay_time_micros);
  flush_thread_ = new std::thread(FlushFunction, this);
  RecalcConditions();
};

SpdbMemoryManager::~SpdbMemoryManager() {
  terminated_ = true;
  WakeUpFlush();
  flush_thread_->join();
  delete flush_thread_;
  delete delay_enforcer_;
}

void SpdbMemoryManager::WakeUpFlush() {
  std::unique_lock<std::mutex> lck(mutex_);
  wakeup_cond_.notify_one();
}

void SpdbMemoryManager::RegisterClient(SpdbMemoryManagerClient *_client) {
  std::unique_lock<std::mutex> lck(mutex_);
  assert(clients_.find(_client) == clients_.end());
  clients_.insert(_client);
}

void SpdbMemoryManager::UnRegisterClient(SpdbMemoryManagerClient *_client) {
  std::unique_lock<std::mutex> lck(mutex_);
  auto iter = clients_.find(_client);
  assert(iter != clients_.end());
  clients_.erase(iter);
}

void SpdbMemoryManager::ClientNeedsDelay(SpdbMemoryManagerClient *cl,
                                         size_t factor) {
  std::unique_lock<std::mutex> lck(mutex_);
  auto current_factor = delay_enforcer_->GetDelayFactor();
  // we may do some optimiztion  here but whatfor?

  cl->SetDelay(factor);
  auto max_factor = factor;
  if (max_factor < current_factor) {
    // otherwise no need to check the clients as the current > cl->max...
    max_factor = memsize_delay_factor_;
    for (auto c : clients_) {
      auto cur_factor = c->GetDelay();
      if (cur_factor > max_factor) {
        max_factor = cur_factor;
      }
    }
  }
  if (current_factor != max_factor) {
    delay_enforcer_->SetDelayFactor(max_factor);
  }
}

void SpdbMemoryManager::SetMemSizeDelayFactor(size_t factor) {
  if (memsize_delay_factor_ != factor) {
    auto current_factor = delay_enforcer_->GetDelayFactor();
    memsize_delay_factor_ = factor;

    auto max_factor = factor;
    if (max_factor < current_factor) {
      // otherwise no need to check the clientls
      max_factor = memsize_delay_factor_;
      for (auto c : clients_) {
        auto cur_factor = c->GetDelay();
        if (cur_factor > max_factor) {
          max_factor = cur_factor;
        }
      }
    }
    if (current_factor != max_factor) {
      delay_enforcer_->SetDelayFactor(max_factor);
    }
  }
}

size_t SpdbMemoryManager::GetDelayedRate() const {
  return delay_enforcer_->GetDelayedRate();
}

void SpdbMemoryManager::EnforceDelay(size_t byte_count) {
  delay_enforcer_->Enforce(byte_count);
}

void SpdbMemoryManager::MaybeInitiateFlushRequest(int mem_full_rate) {
  static const size_t kMinSizeToFlush = 0;

  if (mem_full_rate < kminFlushPrecent) return;

  int n_flushes_required = (mem_full_rate - kminFlushPrecent) *
                               (n_parallel_flushes_) /
                               (100 - kminFlushPrecent) +
                           1;
  if (n_flushes_required > n_parallel_flushes_) {
    n_flushes_required = n_parallel_flushes_;
  }
  n_flushes_required -= n_running_flushes_;

  SpdbMemoryManagerClient *best_client = nullptr;
  size_t largest_size = kMinSizeToFlush;

  if (n_flushes_required > n_scheduled_flushes_) {
    for (auto client : clients_) {
      size_t flush_size =
          client->GetUnFlushDataSize() + client->GetMutableDataSize();
      if (flush_size > largest_size) {
        best_client = client;
        largest_size = flush_size;
      }
    }
    if (best_client) {
      n_scheduled_flushes_++;
      clients_need_flush_.push_back(best_client);
    }
  }
  if (!clients_need_flush_.empty()) {
    wakeup_cond_.notify_one();
  }
}

void SpdbMemoryManager::ReserveMem(const size_t alloc_size) {
  WriteBufferManager::ReserveMem(alloc_size);
  if (memory_usage() > next_recalc_size_) {
    std::unique_lock<std::mutex> lck(mutex_);
    if (memory_usage() > next_recalc_size_) {
      RecalcConditions();
    }
  }
}

void SpdbMemoryManager::FreeMem(size_t mem_size) {
  WriteBufferManager::FreeMem(mem_size);
  std::unique_lock<std::mutex> lck(mutex_);
  RecalcConditions();
}

void SpdbMemoryManager::FlushStarted(const size_t size) {
  std::unique_lock<std::mutex> lck(mutex_);
  size_pendding_for_flush_ += size;
  if (n_scheduled_flushes_ > 0) n_scheduled_flushes_--;
  n_running_flushes_++;
}

void SpdbMemoryManager::FlushEnded(size_t _size) {
  std::unique_lock<std::mutex> lck(mutex_);
  assert(n_running_flushes_ > 0);
  --n_running_flushes_;
  size_pendding_for_flush_ -= _size;
  RecalcConditions();
}

void SpdbMemoryManager::SetBufferSize(size_t _memory_size) {
  std::unique_lock<std::mutex> lck(mutex_);
  WriteBufferManager::SetBufferSize(_memory_size);
  RecalcConditions();
}

void SpdbMemoryManager::RecalcConditions() {
  const int start_delay_permille = 800;
  // more accurate calculations are  needed for the delay
  const int start_flush_permille = kminFlushPrecent * 10;
  size_t delay_factor = 0;
  int rate_full = 1000 * memory_usage() / buffer_size();
  if (rate_full < start_flush_permille) {
    next_recalc_size_ = buffer_size() * start_flush_permille / 1000;
  } else {
    if (rate_full < start_delay_permille) {
      //
      next_recalc_size_ = buffer_size() * (rate_full + 100) / 1000;
    } else {
      // prparing for delay update on every 1%
      next_recalc_size_ = buffer_size() * (rate_full + 10) / 1000;
      delay_factor = rate_full - start_delay_permille;
    }
    MaybeInitiateFlushRequest(rate_full / 10);
  }
  SetMemSizeDelayFactor(delay_factor);
}

// flush thread
void SpdbMemoryManager::FlushFunction(SpdbMemoryManager *me) {
  me->FlushLoop();
}

void SpdbMemoryManager::FlushLoop() {
  while (1) {
    // wait for condition
    {
      std::unique_lock<std::mutex> lck(mutex_);
      if (!terminated_ && clients_need_flush_.empty()) {
        wakeup_cond_.wait(lck);
      }
    }
    if (terminated_) {
      break;
    }

    while (!clients_need_flush_.empty()) {
      SpdbMemoryManagerClient *client = nullptr;
      {
        std::unique_lock<std::mutex> lck(mutex_);
        if (!clients_need_flush_.empty()) {
          client = clients_need_flush_.front();
          clients_need_flush_.pop_front();
        }
      }
      if (client) client->db()->InitiateMemoryManagerFlushRequest(client->cf());
    }
  }
}

SpdbMemoryManager *NewSpdbMemoryManager(
    const SpdbMemoryManagerOptions &options) {
  return new SpdbMemoryManager(options);
}

DelayEnforcer::DelayEnforcer(std::shared_ptr<SystemClock> clock,
                             size_t delayed_write_rate,
                             size_t max_delay_time_micros)
    : clock_(std::move(clock)),
      delayed_write_rate_(delayed_write_rate),
      delay_factor_(0),
      rate_multiplier_(0),
      max_delay_time_nanos_(std::chrono::duration_cast<Nanoseconds>(
          std::chrono::microseconds(max_delay_time_micros))),
      delay_per_byte_nanos_(0)

{
  if (max_delay_time_nanos_.count() == 0) {
    max_delay_time_nanos_ = kNanosPerSec;
  }
  if (delayed_write_rate_ == 0) {
    delayed_write_rate_ = 512 * 1024 * 1024;
  }
}

void DelayEnforcer::Enforce(size_t byte_count) {
  if (delay_per_byte_nanos_ > 0) {
    const auto start_time = Nanoseconds(clock_->NowNanos());
    const auto delay_mul = 1.0 + (1.0 / (kNanosPerSec.count() * byte_count));
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
    const auto sleep_time =
        std::min(request_time - start_time, max_delay_time_nanos_);
    if (sleep_time.count() > 0 && sleep_time > kSleepMin) {
      const auto sleep_micros =
          std::chrono::duration_cast<std::chrono::microseconds>(
              Nanoseconds(sleep_time))
              .count();
      clock_->SleepForMicroseconds(int(sleep_micros));
    }
  }
}

void DelayEnforcer::SetDelayFactor(size_t new_factor) {
  if (new_factor == 0) {
    delay_per_byte_nanos_ = 0;
    delay_factor_ = 0;
    rate_multiplier_ = 0;
  } else {
    auto new_mult = 10000;
    if (new_factor <= SpdbMemoryManager::MaxDelayFactor) {
      new_mult = pow(1.05, new_factor);  // using the current constant it will
                                         // lead to up to e ^ 10 slower rate
    }

    if (new_mult < rate_multiplier_ || rate_multiplier_ == 0) {
      next_request_time_nanos_.store(clock_->NowNanos(),
                                     std::memory_order_release);
    }
    delay_factor_ = new_factor;
    rate_multiplier_ = new_mult;
    delay_per_byte_nanos_.store(
        rate_multiplier_ * kNanosPerSec.count() / delayed_write_rate_,
        std::memory_order_release);
  }
}
}  // namespace ROCKSDB_NAMESPACE
