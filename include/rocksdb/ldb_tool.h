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

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// An interface for converting a slice to a readable string
class SliceFormatter {
 public:
  virtual ~SliceFormatter() {}
  virtual std::string Format(const Slice& s) const = 0;
};

// Options for customizing ldb tool (beyond the DB Options)
struct LDBOptions {
  // Create LDBOptions with default values for all fields
  LDBOptions();

  // Key formatter that converts a slice to a readable string.
  // Default: Slice::ToString()
  std::shared_ptr<SliceFormatter> key_formatter;

  std::string print_help_header = "ldb - Speedb Tool";
};

class LDBTool {
 public:
  void Run(int argc, char** argv, Options db_options = Options(),
           const LDBOptions& ldb_options = LDBOptions(),
           const std::vector<ColumnFamilyDescriptor>* column_families = nullptr,
           bool exit_with_retcode = true);
};

}  // namespace ROCKSDB_NAMESPACE

