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

#include "rocksdb/table_pinning_policy.h"
#include "table/block_based/cachable_entry.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

class BlockBasedTable;
struct BlockCacheLookupContext;
class FilePrefetchBuffer;
class GetContext;
struct ReadOptions;
struct UncompressionDict;

// Provides access to the uncompression dictionary regardless of whether
// it is owned by the reader or stored in the cache, or whether it is pinned
// in the cache or not.
class UncompressionDictReader {
 public:
  static Status Create(
      const BlockBasedTable* table, const ReadOptions& ro,
      const TablePinningOptions& tpo, FilePrefetchBuffer* prefetch_buffer,
      bool use_cache, bool prefetch, bool pin,
      BlockCacheLookupContext* lookup_context,
      std::unique_ptr<UncompressionDictReader>* uncompression_dict_reader);
  ~UncompressionDictReader();
  Status GetOrReadUncompressionDictionary(
      FilePrefetchBuffer* prefetch_buffer, bool no_io, bool verify_checksums,
      GetContext* get_context, BlockCacheLookupContext* lookup_context,
      CachableEntry<UncompressionDict>* uncompression_dict) const;

  size_t ApproximateMemoryUsage() const;

 private:
  UncompressionDictReader(const BlockBasedTable* t,
                          CachableEntry<UncompressionDict>&& uncompression_dict,
                          std::unique_ptr<PinnedEntry>&& pinned)
      : table_(t),
        uncompression_dict_(std::move(uncompression_dict)),
        pinned_(std::move(pinned)) {
    assert(table_);
  }

  bool cache_dictionary_blocks() const;

  static Status ReadUncompressionDictionary(
      const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
      const ReadOptions& read_options, bool use_cache, GetContext* get_context,
      BlockCacheLookupContext* lookup_context,
      CachableEntry<UncompressionDict>* uncompression_dict);

  const BlockBasedTable* table_;
  CachableEntry<UncompressionDict> uncompression_dict_;
  std::unique_ptr<PinnedEntry> pinned_;
};

}  // namespace ROCKSDB_NAMESPACE
