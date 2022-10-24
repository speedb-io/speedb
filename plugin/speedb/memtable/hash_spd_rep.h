// Copyright (C) 2022 Speedb Ltd. All rights reserved.
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

#pragma once

#ifndef ROCKSDB_LITE
#include <condition_variable>  // std::condition_variable
#include <mutex>
#include <thread>

#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {

class HashSpdRepFactory : public MemTableRepFactory {
 public:
  explicit HashSpdRepFactory(size_t bucket_count = 1000000);
  ~HashSpdRepFactory() override;

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override;
  bool IsInsertConcurrentlySupported() const override { return true; }
  bool CanHandleDuplicatedKey() const override { return true; }

  static const char* kClassName() { return "speedb.HashSpdRepFactory"; }
  const char* Name() const override { return kClassName(); }

 private:
  void PrepareSwitchMemTable();
  MemTableRep* GetSwitchMemtable(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator);

 private:
  size_t bucket_count_;
  std::thread switch_memtable_thread_;
  std::mutex switch_memtable_thread_mutex_;
  std::condition_variable switch_memtable_thread_cv_;
  bool terminate_switch_memtable_ = false;
  std::atomic<MemTableRep*> switch_mem_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE