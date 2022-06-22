#include <rocksdb/write_buffer_manager.h>
#include <rocksdb/options.h>
#include "db/db_impl/db_impl.h"
#include <math.h>
#include "memory_manager.h"

namespace ROCKSDB_NAMESPACE {

SpdbMemoryManager::SpdbMemoryManager(const SpdbMemoryManagerOptions &options)
    : WriteBufferManager(options.dirty_data_size),
      cache_(options.cache),
      n_parallel_flushes_(options.n_parallel_flushes),
      n_running_flushes_(0),
      n_scheduled_flushes_(0),
      size_pendding_for_flush_(0),
      terminated_(false) {
  flush_thread_ = new std::thread(FlushFunction, this);
  RecalcConditions();
};

SpdbMemoryManager::~SpdbMemoryManager()
{
  terminated_=true;
  WakeUpFlush();
  flush_thread_->join();
  delete flush_thread_;
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

void SpdbMemoryManager::MaybeInitiateFlushRequest(int mem_full_rate) {
  static const size_t kMinSizeToFlush = 0;
  
  if (mem_full_rate < kminFlushPrecent)
    return;
  
  int n_flushes_required = (mem_full_rate - kminFlushPrecent) *
    (n_parallel_flushes_) / (100 - kminFlushPrecent) + 1;
  if (n_flushes_required > n_parallel_flushes_) {
    n_flushes_required = n_parallel_flushes_;
  }
  n_flushes_required -= n_running_flushes_;

  SpdbMemoryManagerClient *best_client = nullptr;
  size_t largest_size = kMinSizeToFlush; 
  
  if (n_flushes_required > n_scheduled_flushes_) {
    for (auto client : clients_) {
      size_t flush_size = client->GetUnFlushDataSize() + client->GetMutableDataSize();
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
  if (n_scheduled_flushes_ > 0) 
    n_scheduled_flushes_--;
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

  int rate_full = 1000 * memory_usage() / buffer_size();
  if (rate_full < start_flush_permille) {
    next_recalc_size_ = buffer_size() * start_flush_permille / 1000;    
  } else {
    if (rate_full < start_delay_permille) {
      // 
      next_recalc_size_ = buffer_size() * (rate_full + 100) / 1000;
    } else  {
      // prparing for delay update on every 1% 
      next_recalc_size_ = buffer_size() * (rate_full + 10) / 1000;
    }
    MaybeInitiateFlushRequest(rate_full / 10);
  } 
}



// flush thread
void SpdbMemoryManager::FlushFunction(SpdbMemoryManager *me) {
  me->FlushLoop();
}
  

void SpdbMemoryManager::FlushLoop() {
  while(1) {
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
      if (client)
        client->db()->InitiateMemoryManagerFlushRequest(client->cf());
    }
  }
}

SpdbMemoryManager *NewSpdbMemoryManager(
    const SpdbMemoryManagerOptions &options) {
  return new SpdbMemoryManager(options);
}

}
