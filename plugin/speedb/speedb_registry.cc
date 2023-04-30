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

#include "plugin/speedb/speedb_registry.h"

#include "paired_filter/speedb_paired_bloom.h"
#include "plugin/speedb/memtable/hash_spd_rep.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// Similar to the NewBuiltinFilterPolicyWithBits template for RocksDB built-in
// filters
SpdbPairedBloomFilterPolicy* NewSpdbPairedBloomFilterWithBits(
    const std::string& uri) {
  return new SpdbPairedBloomFilterPolicy(
      FilterPolicy::ExtractBitsPerKeyFromUri(uri));
}

int register_SpeedbPlugins(ObjectLibrary& library, const std::string&) {
  library.AddFactory<MemTableRepFactory>(
      ObjectLibrary::PatternEntry(HashSpdRepFactory::kClassName(), true)
          .AddNumber(":"),
      [](const std::string& uri, std::unique_ptr<MemTableRepFactory>* guard,
         std::string* /*errmsg*/) {
        auto colon = uri.find(":");
        if (colon != std::string::npos) {
          size_t buckets = ParseSizeT(uri.substr(colon + 1));
          guard->reset(new HashSpdRepFactory(buckets));
        } else {
          guard->reset(new HashSpdRepFactory());
        }
        return guard->get();
      });

  library.AddFactory<const FilterPolicy>(
      ObjectLibrary::PatternEntry(SpdbPairedBloomFilterPolicy::kClassName(),
                                  false)
          .AnotherName(SpdbPairedBloomFilterPolicy::kNickName())
          .AddNumber(":", false),
      [](const std::string& uri, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(NewSpdbPairedBloomFilterWithBits(uri));
        return guard->get();
      });

  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}

}  // namespace ROCKSDB_NAMESPACE
