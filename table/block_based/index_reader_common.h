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
#pragma once

#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/reader_common.h"

namespace ROCKSDB_NAMESPACE {
struct PinnedEntry;

// Encapsulates common functionality for the various index reader
// implementations. Provides access to the index block regardless of whether
// it is owned by the reader or stored in the cache, or whether it is pinned
// in the cache or not.
class BlockBasedTable::IndexReaderCommon : public BlockBasedTable::IndexReader {
 public:
  IndexReaderCommon(const BlockBasedTable* t,
                    CachableEntry<Block>&& index_block,
                    std::unique_ptr<PinnedEntry>&& pinned)
      : table_(t),
        index_block_(std::move(index_block)),
        pinned_(std::move(pinned)) {
    assert(table_ != nullptr);
  }

  ~IndexReaderCommon() override;

 protected:
  static Status ReadIndexBlock(const BlockBasedTable* table,
                               FilePrefetchBuffer* prefetch_buffer,
                               const ReadOptions& read_options, bool use_cache,
                               GetContext* get_context,
                               BlockCacheLookupContext* lookup_context,
                               CachableEntry<Block>* index_block);

  const BlockBasedTable* table() const { return table_; }

  const InternalKeyComparator* internal_comparator() const {
    assert(table_ != nullptr);
    assert(table_->get_rep() != nullptr);

    return &table_->get_rep()->internal_comparator;
  }

  bool index_has_first_key() const {
    assert(table_ != nullptr);
    assert(table_->get_rep() != nullptr);
    return table_->get_rep()->index_has_first_key;
  }

  bool index_key_includes_seq() const {
    assert(table_ != nullptr);
    assert(table_->get_rep() != nullptr);
    return table_->get_rep()->index_key_includes_seq;
  }

  bool index_value_is_full() const {
    assert(table_ != nullptr);
    assert(table_->get_rep() != nullptr);
    return table_->get_rep()->index_value_is_full;
  }

  bool cache_index_blocks() const {
    assert(table_ != nullptr);
    assert(table_->get_rep() != nullptr);
    return table_->get_rep()->table_options.cache_index_and_filter_blocks;
  }

  Status GetOrReadIndexBlock(bool no_io, Env::IOPriority rate_limiter_priority,
                             GetContext* get_context,
                             BlockCacheLookupContext* lookup_context,
                             CachableEntry<Block>* index_block) const;

  size_t ApproximateIndexBlockMemoryUsage() const {
    assert(!index_block_.GetOwnValue() || index_block_.GetValue() != nullptr);
    return index_block_.GetOwnValue()
               ? index_block_.GetValue()->ApproximateMemoryUsage()
               : 0;
  }

 private:
  const BlockBasedTable* table_;
  CachableEntry<Block> index_block_;
  std::unique_ptr<PinnedEntry> pinned_;
};

}  // namespace ROCKSDB_NAMESPACE
