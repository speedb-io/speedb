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

//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/cache_entry_roles.h"

#include <mutex>

#include "port/lang.h"

namespace ROCKSDB_NAMESPACE {

std::array<std::string, kNumCacheEntryRoles> kCacheEntryRoleToCamelString{{
    "DataBlock",
    "FilterBlock",
    "FilterMetaBlock",
    "DeprecatedFilterBlock",
    "IndexBlock",
    "OtherBlock",
    "WriteBuffer",
    "CompressionDictionaryBuildingBuffer",
    "FilterConstruction",
    "BlockBasedTableReader",
    "FileMetadata",
    "BlobValue",
    "BlobCache",
    "Misc",
}};

std::array<std::string, kNumCacheEntryRoles> kCacheEntryRoleToHyphenString{{
    "data-block",
    "filter-block",
    "filter-meta-block",
    "deprecated-filter-block",
    "index-block",
    "other-block",
    "write-buffer",
    "compression-dictionary-building-buffer",
    "filter-construction",
    "block-based-table-reader",
    "file-metadata",
    "blob-value",
    "blob-cache",
    "misc",
}};

const std::string& GetCacheEntryRoleName(CacheEntryRole role) {
  return kCacheEntryRoleToHyphenString[static_cast<size_t>(role)];
}

const std::string& BlockCacheEntryStatsMapKeys::CacheId() {
  static const std::string kCacheId = "id";
  return kCacheId;
}

const std::string& BlockCacheEntryStatsMapKeys::CacheCapacityBytes() {
  static const std::string kCacheCapacityBytes = "capacity";
  return kCacheCapacityBytes;
}

const std::string&
BlockCacheEntryStatsMapKeys::LastCollectionDurationSeconds() {
  static const std::string kLastCollectionDurationSeconds =
      "secs_for_last_collection";
  return kLastCollectionDurationSeconds;
}

const std::string& BlockCacheEntryStatsMapKeys::LastCollectionAgeSeconds() {
  static const std::string kLastCollectionAgeSeconds =
      "secs_since_last_collection";
  return kLastCollectionAgeSeconds;
}

namespace {

std::string GetPrefixedCacheEntryRoleName(const std::string& prefix,
                                          CacheEntryRole role) {
  const std::string& role_name = GetCacheEntryRoleName(role);
  std::string prefixed_role_name;
  prefixed_role_name.reserve(prefix.size() + role_name.size());
  prefixed_role_name.append(prefix);
  prefixed_role_name.append(role_name);
  return prefixed_role_name;
}

}  // namespace

std::string BlockCacheEntryStatsMapKeys::EntryCount(CacheEntryRole role) {
  const static std::string kPrefix = "count.";
  return GetPrefixedCacheEntryRoleName(kPrefix, role);
}

std::string BlockCacheEntryStatsMapKeys::UsedBytes(CacheEntryRole role) {
  const static std::string kPrefix = "bytes.";
  return GetPrefixedCacheEntryRoleName(kPrefix, role);
}

std::string BlockCacheEntryStatsMapKeys::UsedPercent(CacheEntryRole role) {
  const static std::string kPrefix = "percent.";
  return GetPrefixedCacheEntryRoleName(kPrefix, role);
}

const std::string& BlockCacheCfStatsMapKeys::CfName() {
  static const std::string kCfName = "cf_name";
  return kCfName;
}

const std::string& BlockCacheCfStatsMapKeys::CacheId() {
  static const std::string kCacheId = "id";
  return kCacheId;
}

std::string BlockCacheCfStatsMapKeys::UsedBytes(CacheEntryRole role) {
  const static std::string kPrefix = "bytes.";
  return GetPrefixedCacheEntryRoleName(kPrefix, role);
}

}  // namespace ROCKSDB_NAMESPACE
