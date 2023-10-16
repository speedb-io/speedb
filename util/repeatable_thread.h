// Copyright (C) 2023 Speedb Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <functional>
#include <string>

#include "monitoring/instrumented_mutex.h"
#include "port/port.h"
#include "rocksdb/system_clock.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// Simple wrapper around port::Thread that supports calling a callback every
// X seconds. If you pass in 0, then it will call your callback repeatedly
// without delay.
class RepeatableThread {
 public:
  RepeatableThread(std::function<void()> function,
                   const std::string& thread_name, SystemClock* clock,
                   uint64_t delay_us, uint64_t initial_delay_us = 0)
      : function_(function),
        thread_name_("speedb:" + thread_name),
        clock_(clock),
        delay_us_(delay_us),
        initial_delay_us_(initial_delay_us),
        mutex_(clock),
        cond_var_(&mutex_),
        running_(true),
#ifndef NDEBUG
        waiting_(false),
        run_count_(0),
#endif
        thread_([this] { thread(); }) {
  }

  void cancel() {
    {
      InstrumentedMutexLock l(&mutex_);
      if (!running_) {
        return;
      }
      running_ = false;
      cond_var_.SignalAll();
    }
    thread_.join();
  }

  bool IsRunning() { return running_; }

  ~RepeatableThread() { cancel(); }

#ifndef NDEBUG
  // Wait until RepeatableThread starting waiting, call the optional callback,
  // then wait for one run of RepeatableThread. Tests can use provide a
  // custom clock object to mock time, and use the callback here to bump current
  // time and trigger RepeatableThread. See repeatable_thread_test for example.
  //
  // Note: only support one caller of this method.
  void TEST_WaitForRun(std::function<void()> callback = nullptr) {
    InstrumentedMutexLock l(&mutex_);
    while (!waiting_) {
      cond_var_.Wait();
    }
    uint64_t prev_count = run_count_;
    if (callback != nullptr) {
      callback();
    }
    cond_var_.SignalAll();
    while (!(run_count_ > prev_count)) {
      cond_var_.Wait();
    }
  }
#endif

 private:
  bool wait(uint64_t delay) {
    InstrumentedMutexLock l(&mutex_);
    if (running_ && delay > 0) {
      uint64_t wait_until = clock_->NowMicros() + delay;
#ifndef NDEBUG
      waiting_ = true;
      cond_var_.SignalAll();
#endif
      while (running_) {
        cond_var_.TimedWait(wait_until);
        if (clock_->NowMicros() >= wait_until) {
          break;
        }
      }
#ifndef NDEBUG
      waiting_ = false;
#endif
    }
    return running_;
  }

  void thread() {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    // Set thread name.
    int ret __attribute__((__unused__)) =
        pthread_setname_np(pthread_self(), thread_name_.c_str());
    assert(ret == 0);
#endif
#endif

    assert(delay_us_ > 0);
    if (!wait(initial_delay_us_)) {
      return;
    }
    do {
      function_();
#ifndef NDEBUG
      {
        InstrumentedMutexLock l(&mutex_);
        run_count_++;
        cond_var_.SignalAll();
      }
#endif
    } while (wait(delay_us_));
  }

  const std::function<void()> function_;
  const std::string thread_name_;
  SystemClock* clock_;
  const uint64_t delay_us_;
  const uint64_t initial_delay_us_;

  // Mutex lock should be held when accessing running_, waiting_
  // and run_count_.
  InstrumentedMutex mutex_;
  InstrumentedCondVar cond_var_;
  bool running_;
#ifndef NDEBUG
  // RepeatableThread waiting for timeout.
  bool waiting_;
  // Times function_ had run.
  uint64_t run_count_;
#endif
  port::Thread thread_;
};

}  // namespace ROCKSDB_NAMESPACE
