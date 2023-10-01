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

#include "utilities/nosync_fs.h"

#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static std::unordered_map<std::string, OptionTypeInfo> no_sync_fs_option_info =
    {

        {"sync",
         {offsetof(struct NoSyncOptions, do_sync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
        {"fsync",
         {offsetof(struct NoSyncOptions, do_fsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
        {"range_sync",
         {offsetof(struct NoSyncOptions, do_rsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
        {"dir_sync",
         {offsetof(struct NoSyncOptions, do_dsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},

};
}  // namespace

NoSyncFileSystem::NoSyncFileSystem(const std::shared_ptr<FileSystem>& base,
                                   bool enabled)
    : InjectionFileSystem(base), sync_opts_(enabled) {
  RegisterOptions(&sync_opts_, &no_sync_fs_option_info);
}
}  // namespace ROCKSDB_NAMESPACE
