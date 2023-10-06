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
#include "table/block_based/binary_search_index_reader.h"

#include "rocksdb/table_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {
Status BinarySearchIndexReader::Create(
    const BlockBasedTable* table, const ReadOptions& ro,
    const TablePinningOptions& tpo, FilePrefetchBuffer* prefetch_buffer,
    bool use_cache, bool prefetch, bool pin,
    BlockCacheLookupContext* lookup_context,
    std::unique_ptr<IndexReader>* index_reader) {
  assert(table != nullptr);
  assert(index_reader != nullptr);

  std::unique_ptr<PinnedEntry> pinned;
  CachableEntry<Block> index_block;
  if (prefetch || pin || !use_cache) {
    const Status s =
        ReadIndexBlock(table, prefetch_buffer, ro, use_cache,
                       /*get_context=*/nullptr, lookup_context, &index_block);
    if (!s.ok()) {
      return s;
    }

    if (pin) {
      table->PinData(tpo, TablePinningPolicy::kIndex,
                     index_block.GetValue()->ApproximateMemoryUsage(), &pinned);
    }
    if (use_cache && !pinned) {
      index_block.Reset();
    }
  }

  index_reader->reset(new BinarySearchIndexReader(table, std::move(index_block),
                                                  std::move(pinned)));

  return Status::OK();
}

InternalIteratorBase<IndexValue>* BinarySearchIndexReader::NewIterator(
    const ReadOptions& read_options, bool /* disable_prefix_seek */,
    IndexBlockIter* iter, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
  const BlockBasedTable::Rep* rep = table()->get_rep();
  const bool no_io = (read_options.read_tier == kBlockCacheTier);
  CachableEntry<Block> index_block;
  const Status s =
      GetOrReadIndexBlock(no_io, read_options.rate_limiter_priority,
                          get_context, lookup_context, &index_block);
  if (!s.ok()) {
    if (iter != nullptr) {
      iter->Invalidate(s);
      return iter;
    }

    return NewErrorInternalIterator<IndexValue>(s);
  }

  Statistics* kNullStats = nullptr;
  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  auto it = index_block.GetValue()->NewIndexIterator(
      internal_comparator()->user_comparator(),
      rep->get_global_seqno(BlockType::kIndex), iter, kNullStats, true,
      index_has_first_key(), index_key_includes_seq(), index_value_is_full());

  assert(it != nullptr);
  index_block.TransferTo(it);

  return it;
}
}  // namespace ROCKSDB_NAMESPACE
