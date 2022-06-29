//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <mutex>
#include <set>
#include <thread>

#include "rocksdb/cache.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
class CacheReservationManager;

// Interface to block and signal DB instances, intended for RocksDB
// internal use only. Each DB instance contains ptr to StallInterface.
class StallInterface {
 public:
  virtual ~StallInterface() {}

  virtual void Block() = 0;

  virtual void Signal() = 0;
};

// write buffer manager was expanded using spdb_memory_manager
class WriteBufferManager {
 public:
  // Parameters:
  // _buffer_size: _buffer_size = 0 indicates no limit. Memory won't be capped.
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  //
  // cache_: if `cache` is provided, we'll put dummy entries in the cache and
  // cost the memory allocated to the cache. It can be used even if _buffer_size
  // = 0.
  //
  // allow_stall: if set true, it will enable stalling of writes when
  // memory_usage() exceeds buffer_size. It will wait for flush to complete and
  // memory usage to drop down.
  explicit WriteBufferManager(size_t _buffer_size,
                              std::shared_ptr<Cache> cache = {},
                              bool allow_stall = false);
  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  virtual ~WriteBufferManager();

  // Returns true if buffer_limit is passed to limit the total memory usage and
  // is greater than 0.
  bool enabled() const { return buffer_size() > 0; }

  // Returns true if pointer to cache is passed.
  bool cost_to_cache() const { return cache_res_mgr_ != nullptr; }

  // Returns the total memory used by memtables.
  // Only valid if enabled()
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

  // Returns the total memory used by active memtables.
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }

  size_t dummy_entries_in_cache_usage() const;

  // Returns the buffer_size.
  size_t buffer_size() const {
    return buffer_size_.load(std::memory_order_relaxed);
  }

  virtual void SetBufferSize(size_t new_size) {
    buffer_size_.store(new_size, std::memory_order_relaxed);
    mutable_limit_.store(new_size * 7 / 8, std::memory_order_relaxed);
    // Check if stall is active and can be ended.
    MaybeEndWriteStall();
  }

  // Below functions should be called by RocksDB internally.

  // Should only be called from write thread
  virtual bool ShouldFlush() const {
    if (enabled()) {
      if (mutable_memtable_memory_usage() >
          mutable_limit_.load(std::memory_order_relaxed)) {
        return true;
      }
      size_t local_size = buffer_size();
      if (memory_usage() >= local_size &&
          mutable_memtable_memory_usage() >= local_size / 2) {
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        return true;
      }
    }
    return false;
  }

  // Returns true if total memory usage exceeded buffer_size.
  // We stall the writes untill memory_usage drops below buffer_size. When the
  // function returns true, all writer threads (including one checking this
  // condition) across all DBs will be stalled. Stall is allowed only if user
  // pass allow_stall = true during WriteBufferManager instance creation.
  //
  // Should only be called by RocksDB internally .
  virtual bool ShouldStall() const {
    if (!allow_stall_ || !enabled()) {
      return false;
    }

    return IsStallActive() || IsStallThresholdExceeded();
  }

  // Returns true if stall is active.
  bool IsStallActive() const {
    return stall_active_.load(std::memory_order_relaxed);
  }

  // Returns true if stalling condition is met.
  bool IsStallThresholdExceeded() const {
    return memory_usage() >= buffer_size_;
  }

  virtual void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem);

  virtual void FreeMem(size_t mem);

  // Add the DB instance to the queue and block the DB.
  // Should only be called by RocksDB internally.
  void BeginWriteStall(StallInterface* wbm_stall);

  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  void MaybeEndWriteStall();

  void RemoveDBFromQueue(StallInterface* wbm_stall);

 private:
  std::atomic<size_t> buffer_size_;
  std::atomic<size_t> mutable_limit_;
  std::atomic<size_t> memory_used_;
  // Memory that hasn't been scheduled to free.
  std::atomic<size_t> memory_active_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
  // Protects cache_res_mgr_
  std::mutex cache_res_mgr_mu_;

  std::list<StallInterface *> queue_;
  // Protects the queue_ and stall_active_.
  std::mutex mu_;
  bool allow_stall_;
  // Value should only be changed by BeginWriteStall() and MaybeEndWriteStall()
  // while holding mu_, but it can be read without a lock.
  std::atomic<bool> stall_active_;

  void ReserveMemWithCache(size_t mem);
  void FreeMemWithCache(size_t mem);
};

struct SpdbMemoryManagerOptions {
  SpdbMemoryManagerOptions(){};
  SpdbMemoryManagerOptions(std::shared_ptr<Cache> _cache,
                           std::shared_ptr<SystemClock> _clock,
                           size_t _dirty_data_size = 1ul << 30,
                           size_t _n_parallel_flushes = 4,
                           size_t _max_pinned_data_ratio = 90,
                           size_t _delayed_write_rate = 1ul << 40,
                           size_t _max_delay_time_micros = 1000 * 1000)
      : cache(_cache),
        clock(_clock),
        dirty_data_size(_dirty_data_size),
        n_parallel_flushes(_n_parallel_flushes),
        max_pinned_data_ratio(_max_pinned_data_ratio),
        delayed_write_rate(_delayed_write_rate),
        max_delay_time_micros(_max_delay_time_micros)

  {}

  // clean data
  std::shared_ptr<Cache> cache;
  std::shared_ptr<SystemClock> clock;
  // dirty data size
  size_t dirty_data_size = 1ul << 30;
  size_t n_parallel_flushes = 4;
  size_t max_pinned_data_ratio = 90;
  size_t delayed_write_rate = 1ul << 40;
  size_t max_delay_time_micros = 1000 * 1000;
};

// spdb memory manager is responsible for memory mangement and timing of the
// flushes between multiple CF
//
class SpdbMemoryManagerClient;
class LogBuffer;
class DelayEnforcer;

class SpdbMemoryManager : public WriteBufferManager {
 public:
 public:
  SpdbMemoryManager(const SpdbMemoryManagerOptions &options);
  ~SpdbMemoryManager();

 public:
  void SetBufferSize(size_t new_size) override;

 public:
  // return always false
  bool ShouldFlush() const override { return false; }
  bool ShouldStall() const override { return false; }

 public:
  void ReserveMem(size_t mem) override;
  void FreeMem(size_t mem) override;

  void RegisterClient(SpdbMemoryManagerClient *);
  void UnRegisterClient(SpdbMemoryManagerClient *);
  void FlushStarted(size_t flush_size);
  void FlushEnded(size_t flush_size);

 public:
  // enforce a delay on write based on size
  void EnforceDelay(size_t write_size_bytes);
  // change the delay of a given client
  void ClientNeedsDelay(SpdbMemoryManagerClient *, size_t delay_factor);
  void SetMemSizeDelayFactor(size_t);
  size_t GetDelayedRate() const;
  static const size_t MaxDelayFactor = 200;

 public:
  std::shared_ptr<Cache> GetCache() { return cache_; }

 private:
  void MaybeInitiateFlushRequest(int mem_full_rate);
  void RecalcConditions();

 private:
  std::shared_ptr<Cache> cache_;
  std::unordered_set<SpdbMemoryManagerClient *> clients_;
  int n_parallel_flushes_;
  int n_running_flushes_;
  int n_scheduled_flushes_;
  size_t size_pendding_for_flush_;
  size_t next_recalc_size_;
  static const int kminFlushPrecent = 40;

 private:
  DelayEnforcer *delay_enforcer_;
  size_t memsize_delay_factor_;

 private:
  // flush threads support (flush must run on a seperate thread to avoid
  // deadlock as the scheduler is called in the context of write and flush ask
  // for quiece of writes
  std::list<SpdbMemoryManagerClient *> clients_need_flush_;
  std::thread *flush_thread_;
  std::mutex mutex_;
  std::condition_variable wakeup_cond_;
  bool terminated_;

 private:
  static void FlushFunction(SpdbMemoryManager *flush_scheduler);
  void FlushLoop();
  void WakeUpFlush();
};
};
