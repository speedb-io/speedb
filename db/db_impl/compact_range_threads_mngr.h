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
//
// This class keeps track of the information about internal threads created to
// handle non-blocking CompactRange() user requests.
// A new internal thread is created for every non-blocking request. This class
// allows the DB to know which threads exist and control their lifetime.

#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "port/port.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Forward Declaration
class CompactRangeCompletedCbIf;

class CompactRangeThreadsMngr {
 public:
  ~CompactRangeThreadsMngr();

  void Shutdown();

  // In addition to adding the thread and callback obj, this method lazily
  // removes, from its container, threads that may be joined (those whose
  // callbacks were already called). Alternatively, this could have been done as
  // a periodic activity in the periodic scheduler, but seems not to be a
  // worthwhile periodic activity.
  void AddThread(port::Thread&& thread,
                 std::shared_ptr<CompactRangeCompletedCbIf> cb_obj);

 private:
  void CleanupCompletedThreads();

 private:
  using ThreadInfo =
      std::pair<port::Thread, std::shared_ptr<CompactRangeCompletedCbIf>>;

 private:
  mutable std::mutex lock_;

  // A list should be fine as there is no random access required
  // and a very small number of threads is expected
  std::list<ThreadInfo> threads_infos_;
};

}  // namespace ROCKSDB_NAMESPACE
