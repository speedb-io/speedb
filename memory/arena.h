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

//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Arena is an implementation of Allocator class. For a request of small size,
// it allocates a block with pre-defined block size. For a request of big
// size, it uses malloc to directly get the requested size.

#pragma once

#include <cstddef>
#include <deque>

#include "memory/allocator.h"
#include "port/mmap.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
struct ArenaTracker {
  // Count must be the last item of the enum
  enum ArenaStats {
    ArenaWrappedDBIter,
    FileIndexerUpdateIndex,
    MemTableNewIterator,
    LevelFileInit,
    FindLevelFileTestAdd,
    DoGenerateLevelFilesBrief,
    TEST_GetLevelIterator,
    VersionAddIteratorsForLevel,
    VersionOverlapWithLevelIterator,
    LogBufferAddLogToBuffer,
    arena_test,
    HashLinkList,
    HashLinkListIterator,
    HashLinkListDynamicIterator,
    HashSkipList,
    HashSkipListIterator,
    HashSkipListDynamicIterator,
    HashSpdb,
    HashSpdbIterator,
    InlineSkipList,
    SkipList,
    SkipListIterator,
    SkipListLookaheadIterator,
    VectorMemtable,
    CompactionMergingIterator,
    NewErrorInternalIterator,
    NewEmptyInternalIterator,
    MergingIterator,
    BlockBasedTableIterator,
    BlockPrefixIndexBuilder,
    CuckooTableIterator,
    PlainTableBloomV1,
    PlainTableIndexBuilderFillIndexes,
    PlainTableReaderNewIterator,
    DynamicBloom,
    DefaultMemtableImpl,
    WriteBatchWithIndex,
    Count
  };
  using stats_pair = std::pair<const char*, std::atomic_uint64_t>;
  std::vector<stats_pair> arena_stats;
  std::atomic_uint64_t total = {0};
  ArenaTracker() : arena_stats(Count) {
    // arena_stats.reserve(ArenaStats::Count);
    //  The order must be the same as the enum ArenaStats order
    int i = 0;
    for (auto const& key : {"ArenaWrappedDBIter",
                            "FileIndexer::UpdateIndex",
                            "MemTable::NewIterator",
                            "LevelFileInit",
                            "FindLevelFileTest::Add",
                            "DoGenerateLevelFilesBrief",
                            "TEST_GetLevelIterator",
                            "Version::AddIteratorsForLevel",
                            "Version::OverlapWithLevelIterator",
                            "LogBuffer::AddLogToBuffer",
                            "arena_test",
                            "HashLinkList",
                            "HashLinkListIterator",
                            "HashLinkListDynamicIterator",
                            "HashSkipList",
                            "HashSkipListIterator",
                            "HashSkipListDynamicIterator",
                            "HashSpdb",
                            "HashSpdbIterator",
                            "InlineSkipList",
                            "SkipList",
                            "SkipListIterator",
                            "SkipListLookaheadIterator",
                            "VectorMemtable",
                            "CompactionMergingIterator",
                            "NewErrorInternalIterator",
                            "NewEmptyInternalIterator",
                            "MergingIterator",
                            "BlockBasedTableIterator",
                            "BlockPrefixIndex::Builder",
                            "CuckooTableIterator",
                            "PlainTableBloomV1",
                            "PlainTableIndexBuilder::FillIndexes",
                            "PlainTableReader::NewIterator",
                            "DynamicBloom",
                            "DefaultMemtableImpl",
                            "WriteBatchWithIndex"}) {
      arena_stats[i].first = key;
      arena_stats[i].second.store(0);
      ++i;
    }

    assert(Count == i);
  };
};
class Arena : public Allocator {
 public:
  // No copying allowed
#ifdef MEMORY_REPORTING
  inline static ArenaTracker arena_tracker_;
#endif
  Arena(const Arena&) = delete;
  void operator=(const Arena&) = delete;

  static constexpr size_t kInlineSize = 2048;
  static constexpr size_t kMinBlockSize = 4096;
  static constexpr size_t kMaxBlockSize = 2u << 30;

  static constexpr unsigned kAlignUnit = alignof(std::max_align_t);
  static_assert((kAlignUnit & (kAlignUnit - 1)) == 0,
                "Pointer size should be power of 2");

  // huge_page_size: if 0, don't use huge page TLB. If > 0 (should set to the
  // supported hugepage size of the system), block allocation will try huge
  // page TLB first. If allocation fails, will fall back to normal case.
  explicit Arena(size_t block_size = kMinBlockSize,
                 AllocTracker* tracker = nullptr, size_t huge_page_size = 0);
  ~Arena();

  char* Allocate(size_t bytes, uint8_t caller_name) override;

  // huge_page_size: if >0, will try to allocate from huage page TLB.
  // The argument will be the size of the page size for huge page TLB. Bytes
  // will be rounded up to multiple of the page size to allocate through mmap
  // anonymous option with huge page on. The extra  space allocated will be
  // wasted. If allocation fails, will fall back to normal case. To enable it,
  // need to reserve huge pages for it to be allocated, like:
  //     sysctl -w vm.nr_hugepages=20
  // See linux doc Documentation/vm/hugetlbpage.txt for details.
  // huge page allocation can fail. In this case it will fail back to
  // normal cases. The messages will be logged to logger. So when calling with
  // huge_page_tlb_size > 0, we highly recommend a logger is passed in.
  // Otherwise, the error message will be printed out to stderr directly.
  char* AllocateAligned(size_t bytes, uint8_t caller_name,
                        size_t huge_page_size = 0,
                        Logger* logger = nullptr) override;

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (exclude the space allocated but not yet used for future
  // allocations).
  size_t ApproximateMemoryUsage() const {
    return blocks_memory_ + blocks_.size() * sizeof(char*) -
           alloc_bytes_remaining_;
  }

  size_t MemoryAllocatedBytes() const { return blocks_memory_; }

  size_t AllocatedAndUnused() const { return alloc_bytes_remaining_; }

  // If an allocation is too big, we'll allocate an irregular block with the
  // same size of that allocation.
  size_t IrregularBlockNum() const { return irregular_block_num; }

  size_t BlockSize() const override { return kBlockSize; }

  bool IsInInlineBlock() const {
    return blocks_.empty() && huge_blocks_.empty();
  }

  // check and adjust the block_size so that the return value is
  //  1. in the range of [kMinBlockSize, kMaxBlockSize].
  //  2. the multiple of align unit.
  static size_t OptimizeBlockSize(size_t block_size);

 private:
  alignas(std::max_align_t) char inline_block_[kInlineSize];
  // Number of bytes allocated in one block
  const size_t kBlockSize;
#ifdef MEMORY_REPORTING
  // Allocated memory blocks
  std::deque<std::pair<std::unique_ptr<char[]>, uint8_t>> blocks_;
  // Huge page allocations
  std::deque<std::pair<MemMapping, std::pair<uint8_t, uint64_t>>> huge_blocks_;
#else
  // Allocated memory blocks
  std::deque<std::unique_ptr<char[]>> blocks_;
  // Huge page allocations
  std::deque<MemMapping> huge_blocks_;
#endif
  size_t irregular_block_num = 0;

  // Stats for current active block.
  // For each block, we allocate aligned memory chucks from one end and
  // allocate unaligned memory chucks from the other end. Otherwise the
  // memory waste for alignment will be higher if we allocate both types of
  // memory from one direction.
  char* unaligned_alloc_ptr_ = nullptr;
  char* aligned_alloc_ptr_ = nullptr;
  // How many bytes left in currently active block?
  size_t alloc_bytes_remaining_ = 0;

  size_t hugetlb_size_ = 0;

  char* AllocateFromHugePage(size_t bytes, uint8_t caller_name);
  char* AllocateFallback(size_t bytes, bool aligned, uint8_t caller_name);
  char* AllocateNewBlock(size_t block_bytes, uint8_t caller_name);

  // Bytes of memory in blocks allocated so far
  size_t blocks_memory_ = 0;
  // Non-owned
  AllocTracker* tracker_;
};

inline char* Arena::Allocate(size_t bytes, uint8_t caller_name) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    unaligned_alloc_ptr_ -= bytes;
    alloc_bytes_remaining_ -= bytes;
    return unaligned_alloc_ptr_;
  }
  return AllocateFallback(bytes, false /* unaligned */, caller_name);
}

}  // namespace ROCKSDB_NAMESPACE
