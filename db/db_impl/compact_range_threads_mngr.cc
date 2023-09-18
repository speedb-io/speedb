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

#include "compact_range_threads_mngr.h"

#include <algorithm>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

CompactRangeThreadsMngr::~CompactRangeThreadsMngr() { Shutdown(); }

void CompactRangeThreadsMngr::Shutdown() {
  std::lock_guard<std::mutex> lock(lock_);

  CleanupCompletedThreads();
  // At this point (shutdown), expecting all objs will have their callbacks
  // called => joined and removed from the list
  assert(threads_infos_.empty());
}

void CompactRangeThreadsMngr::AddThread(
    port::Thread&& thread, std::shared_ptr<CompactRangeCompletedCbIf> cb_obj) {
  std::lock_guard<std::mutex> lock(lock_);

  // Lazy removal (and destruction) of completed threads
  CleanupCompletedThreads();
  threads_infos_.push_back(std::make_pair(std::move(thread), cb_obj));
}

void CompactRangeThreadsMngr::CleanupCompletedThreads() {
  auto threads_infos_iter = begin(threads_infos_);
  while (threads_infos_iter != threads_infos_.end()) {
    auto& thread = threads_infos_iter->first;
    auto& cb_obj = threads_infos_iter->second;

    if (cb_obj->WasCbCalled()) {
      // Thread may safely be joined. Expecting the join() to end
      // immediately (callback as already called).
      thread.join();
      threads_infos_iter = threads_infos_.erase(threads_infos_iter);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
