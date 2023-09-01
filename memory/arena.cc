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
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memory/arena.h"

#include <algorithm>
#include <functional>
#include <utility>

#include "logging/logging.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"
namespace ROCKSDB_NAMESPACE {

size_t Arena::OptimizeBlockSize(size_t block_size) {
  // Make sure block_size is in optimal range
  block_size = std::max(Arena::kMinBlockSize, block_size);
  block_size = std::min(Arena::kMaxBlockSize, block_size);

  // make sure block_size is the multiple of kAlignUnit
  if (block_size % kAlignUnit != 0) {
    block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
  }

  return block_size;
}

Arena::Arena(size_t block_size, AllocTracker* tracker, size_t huge_page_size)
    : kBlockSize(OptimizeBlockSize(block_size)), tracker_(tracker) {
  assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize &&
         kBlockSize % kAlignUnit == 0);
  TEST_SYNC_POINT_CALLBACK("Arena::Arena:0", const_cast<size_t*>(&kBlockSize));
  alloc_bytes_remaining_ = sizeof(inline_block_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = inline_block_;
  unaligned_alloc_ptr_ = inline_block_ + alloc_bytes_remaining_;
  if (MemMapping::kHugePageSupported) {
    hugetlb_size_ = huge_page_size;
    if (hugetlb_size_ && kBlockSize > hugetlb_size_) {
      hugetlb_size_ = ((kBlockSize - 1U) / hugetlb_size_ + 1U) * hugetlb_size_;
    }
  }
  if (tracker_ != nullptr) {
    tracker_->Allocate(kInlineSize);
  }
}

Arena::~Arena() {
#ifdef MEMORY_REPORTING
  for (const auto& itr : blocks_) {
    size_t block_size = malloc_usable_size(
        const_cast<void*>(static_cast<const void*>(itr.first.get())));
    arena_tracker_.arena_stats[itr.second].second.fetch_sub(block_size);
    arena_tracker_.total.fetch_sub(block_size);
  }
  for (const auto& itr : huge_blocks_) {
    size_t block_size = itr.second.second;
    arena_tracker_.arena_stats[itr.second.first].second.fetch_sub(block_size);
    arena_tracker_.total.fetch_sub(block_size);
  }
#endif
  if (tracker_ != nullptr) {
    assert(tracker_->IsMemoryFreed());
    tracker_->FreeMem();
  }
}

char* Arena::AllocateFallback(size_t bytes, bool aligned, uint8_t caller_name) {
  if (bytes > kBlockSize / 4) {
    ++irregular_block_num;
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    return AllocateNewBlock(bytes, caller_name);
  }

  // We waste the remaining space in the current block.
  size_t size = 0;
  char* block_head = nullptr;
  if (MemMapping::kHugePageSupported && hugetlb_size_ > 0) {
    size = hugetlb_size_;
    block_head = AllocateFromHugePage(size, caller_name);
  }
  if (!block_head) {
    size = kBlockSize;
    block_head = AllocateNewBlock(size, caller_name);
  }
  alloc_bytes_remaining_ = size - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + size;
    return block_head;
  } else {
    aligned_alloc_ptr_ = block_head;
    unaligned_alloc_ptr_ = block_head + size - bytes;
    return unaligned_alloc_ptr_;
  }
}

char* Arena::AllocateFromHugePage(size_t bytes,
                                  [[maybe_unused]] uint8_t caller_name) {
  MemMapping mm = MemMapping::AllocateHuge(bytes);
#ifdef MEMORY_REPORTING
  arena_tracker_.arena_stats[caller_name].second.fetch_add(bytes);
  arena_tracker_.total.fetch_add(bytes);
#endif
  auto addr = static_cast<char*>(mm.Get());
  if (addr) {
#ifdef MEMORY_REPORTING
    huge_blocks_.push_back(
        std::make_pair(std::move(mm), std::make_pair(caller_name, bytes)));
#else
    huge_blocks_.push_back(std::move(mm));
#endif
    blocks_memory_ += bytes;
    if (tracker_ != nullptr) {
      tracker_->Allocate(bytes);
    }
  }
  return addr;
}

char* Arena::AllocateAligned(size_t bytes, uint8_t caller_name,
                             size_t huge_page_size, Logger* logger) {
  if (MemMapping::kHugePageSupported && hugetlb_size_ > 0 &&
      huge_page_size > 0 && bytes > 0) {
    // Allocate from a huge page TLB table.
    size_t reserved_size =
        ((bytes - 1U) / huge_page_size + 1U) * huge_page_size;
    assert(reserved_size >= bytes);

    char* addr = AllocateFromHugePage(reserved_size, caller_name);
    if (addr == nullptr) {
      ROCKS_LOG_WARN(logger,
                     "AllocateAligned fail to allocate huge TLB pages: %s",
                     errnoStr(errno).c_str());
      // fail back to malloc
    } else {
      return addr;
    }
  }

  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
  size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returns aligned memory
    result = AllocateFallback(bytes, true /* aligned */, caller_name);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes,
                              [[maybe_unused]] uint8_t caller_name) {
  // NOTE: std::make_unique zero-initializes the block so is not appropriate
  // here
  char* block = new char[block_bytes];
  size_t allocated_size;
#ifdef MEMORY_REPORTING
  allocated_size = malloc_usable_size(block);
  arena_tracker_.arena_stats[caller_name].second.fetch_add(allocated_size);
  arena_tracker_.total.fetch_add(allocated_size);
  blocks_.push_back(
      std::make_pair(std::unique_ptr<char[]>(block), caller_name));
#else
  blocks_.push_back(std::unique_ptr<char[]>(block));
#endif

#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  allocated_size = malloc_usable_size(block);
#ifndef NDEBUG
  // It's hard to predict what malloc_usable_size() returns.
  // A callback can allow users to change the costed size.
  std::pair<size_t*, size_t*> pair(&allocated_size, &block_bytes);
  TEST_SYNC_POINT_CALLBACK("Arena::AllocateNewBlock:0", &pair);
#endif  // NDEBUG
#else
  allocated_size = block_bytes;
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  blocks_memory_ += allocated_size;
  if (tracker_ != nullptr) {
    tracker_->Allocate(allocated_size);
  }
  return block;
}

}  // namespace ROCKSDB_NAMESPACE
