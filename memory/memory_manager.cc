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

bool SpdbMemoryManager::InitiateFlushRequest() {
  static const size_t kMinSizeToFlush =
      n_running_flushes_ == 0 ? 0 : buffer_size() / 10;
  SpdbMemoryManagerClient *best_client = nullptr;
  size_t largest_size = kMinSizeToFlush;

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
   wakeup_cond_.notify_one();
   return true;
 }
 return false;

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
  const int start_delay_percent = 80;
  size_t start_delay_size = buffer_size() * start_delay_percent / 100;  
  size_t start_flush_step = start_delay_size  / n_parallel_flushes_;
  size_t start_flush_size = start_flush_step; 

  size_t delay_factor = 0;
  next_recalc_size_ =
      start_flush_size +
    start_flush_step * (n_running_flushes_ + n_scheduled_flushes_);
  if (memory_usage() >= next_recalc_size_) {
    // need to schedule more 
    if (InitiateFlushRequest()) {
      next_recalc_size_ =
          start_flush_size +
          start_flush_step * (n_running_flushes_ + n_scheduled_flushes_);
    } else {
      // wait 1% extra before intiating one moer
      next_recalc_size_ = memory_usage() + buffer_size() / 100;
    }
  }

  if (next_recalc_size_ > start_delay_size) {
    next_recalc_size_ = memory_usage() + buffer_size() / MaxDelayFactor;
  }
  if (memory_usage() > start_delay_size) {  
    delay_factor = MaxDelayFactor;
    if (memory_usage() < buffer_size()) {
      auto extra_data = memory_usage() - start_delay_size;
      auto max_extra_data = buffer_size() - start_delay_size;
      delay_factor = MaxDelayFactor * extra_data / max_extra_data;
    }
  }

  SetMemSizeDelayFactor(delay_factor);
#if 0
  const size_t mb = 1024 * 1024;
  printf(" %lu %lu %lu %lu %d %d \n",   memory_usage() / mb , next_recalc_size_/mb, delay_factor,
	 GetDelayedRate() / mb, n_running_flushes_, (int) n_scheduled_flushes_ );
#endif
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
      if (client) {
	if (!client->db()->InitiateMemoryManagerFlushRequest(client->cf())) {
	  n_scheduled_flushes_--;
	}
      }
    }
  }
}

SpdbMemoryManager *NewSpdbMemoryManager(
    const SpdbMemoryManagerOptions &options) {
  return new SpdbMemoryManager(options);
}

void OptimizeForSpdbMemoryManager(Options &options) {
  // make sure the mem manager is the sole responsible for flushes
  if (options.write_buffer_size == 0)
    options.write_buffer_size = options.db_write_buffer_size;
  options.max_write_buffer_number =
      (options.db_write_buffer_size / options.write_buffer_size + 1) * 2;
  options.min_write_buffer_number_to_merge =
      options.max_write_buffer_number / 2;
  if (options.level0_slowdown_writes_trigger == 0) {
    int compaction_trigger = options.level0_file_num_compaction_trigger;
    if (compaction_trigger == 0) {
      compaction_trigger = options.level0_file_num_compaction_trigger = 4;
    }
    // generate a widder range between slowdown and stop (for now) to smooth the
    // delay. TBD compaction team to work on smoothing compaction
    options.level0_slowdown_writes_trigger = compaction_trigger * 3;
    options.level0_stop_writes_trigger = compaction_trigger * 8;
  }
  
  options.max_subcompactions = options.max_background_flushes; 
    
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
  if (delay_per_byte_nanos_ > 0 && byte_count > 0) {
    const auto start_time = Nanoseconds(clock_->NowNanos());
    // if we assume that we write at delayed_write_rate / rate_multiplier_
    // this calculation will lead to ~ 25 % decrease per second  while the
    // condition that cause the delay is persistent
    // const double delay_mul = 1;  //+ (1.0 * byte_count * rate_multiplier_ /
                                 //(delayed_write_rate_ * 4));
    const auto current_delay =
        delay_per_byte_nanos_.load(std::memory_order_acquire);
    // We just need the delay per byte to be written atomically, but we don't
    // really care if another thread wins and sets the delay that it calculated.
    //delay_per_byte_nanos_.store(current_delay * delay_mul,
    //                            std::memory_order_release);

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
    auto const kMaxMult = 1000;
    double new_mult = kMaxMult;    
    if (new_factor < SpdbMemoryManager::MaxDelayFactor) {
      new_mult = pow(1.035, new_factor); // 0.035 ^ kmaxDelayFactor =~ 1000  
    }

    if (new_mult < rate_multiplier_ || rate_multiplier_ == 0) {
      next_request_time_nanos_.store(clock_->NowNanos(),
                                     std::memory_order_release);
    }
    delay_factor_ = new_factor;
    rate_multiplier_ = new_mult;
    delay_per_byte_nanos_.store(
        1.0 * rate_multiplier_ * kNanosPerSec.count() / delayed_write_rate_,
        std::memory_order_release);
  }
}
}  // namespace ROCKSDB_NAMESPACE
