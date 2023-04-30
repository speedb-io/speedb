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

#include <condition_variable>  // std::condition_variable
#include <mutex>
#include <thread>

#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {

class HashSpdRepFactory : public MemTableRepFactory {
 public:
  explicit HashSpdRepFactory(size_t bucket_count = 1000000);

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override;
  bool IsInsertConcurrentlySupported() const override { return true; }
  bool CanHandleDuplicatedKey() const override { return true; }
  MemTableRep* PreCreateMemTableRep() override;
  void PostCreateMemTableRep(MemTableRep* switch_mem,
                             const MemTableRep::KeyComparator& compare,
                             Allocator* allocator,
                             const SliceTransform* transform,
                             Logger* logger) override;

  static const char* kClassName() { return "speedb.HashSpdRepFactory"; }
  const char* Name() const override { return kClassName(); }

 private:
  size_t bucket_count_;
  bool use_seek_parralel_threshold_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
