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

#pragma once

#include <cassert>

#include "table/block_based/cachable_entry.h"
#include "table/block_based/filter_block.h"

namespace ROCKSDB_NAMESPACE {

class BlockBasedTable;
class FilePrefetchBuffer;
struct PinnedEntry;

// Encapsulates common functionality for the various filter block reader
// implementations. Provides access to the filter block regardless of whether
// it is owned by the reader or stored in the cache, or whether it is pinned
// in the cache or not.
template <typename TBlocklike>
class FilterBlockReaderCommon : public FilterBlockReader {
 public:
  FilterBlockReaderCommon(const BlockBasedTable* t,
                          CachableEntry<TBlocklike>&& filter_block,
                          std::unique_ptr<PinnedEntry>&& pinned)
      : table_(t),
        filter_block_(std::move(filter_block)),
        pinned_(std::move(pinned)) {
    assert(table_);
    const SliceTransform* const prefix_extractor = table_prefix_extractor();
    if (prefix_extractor) {
      full_length_enabled_ =
          prefix_extractor->FullLengthEnabled(&prefix_extractor_full_length_);
    }
  }
  ~FilterBlockReaderCommon() override;
  bool RangeMayExist(const Slice* iterate_upper_bound, const Slice& user_key,
                     const SliceTransform* prefix_extractor,
                     const Comparator* comparator,
                     const Slice* const const_ikey_ptr, bool* filter_checked,
                     bool need_upper_bound_check, bool no_io,
                     BlockCacheLookupContext* lookup_context,
                     Env::IOPriority rate_limiter_priority) override;

 protected:
  static Status ReadFilterBlock(const BlockBasedTable* table,
                                FilePrefetchBuffer* prefetch_buffer,
                                const ReadOptions& read_options, bool use_cache,
                                GetContext* get_context,
                                BlockCacheLookupContext* lookup_context,
                                CachableEntry<TBlocklike>* filter_block);

  const BlockBasedTable* table() const { return table_; }
  const SliceTransform* table_prefix_extractor() const;
  bool whole_key_filtering() const;
  bool cache_filter_blocks() const;

  Status GetOrReadFilterBlock(bool no_io, GetContext* get_context,
                              BlockCacheLookupContext* lookup_context,
                              CachableEntry<TBlocklike>* filter_block,
                              Env::IOPriority rate_limiter_priority) const;

  size_t ApproximateFilterBlockMemoryUsage() const;

 private:
  bool IsFilterCompatible(const Slice* iterate_upper_bound, const Slice& prefix,
                          const Comparator* comparator) const;

 private:
  const BlockBasedTable* table_;
  CachableEntry<TBlocklike> filter_block_;
  std::unique_ptr<PinnedEntry> pinned_;
  size_t prefix_extractor_full_length_ = 0;
  bool full_length_enabled_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
