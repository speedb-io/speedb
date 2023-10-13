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

#include "utilities/compaction_filters/remove_emptyvalue_compactionfilter.h"

#include <string>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

bool RemoveEmptyValueCompactionFilter::Filter(int /*level*/,
                                              const Slice& /*key*/,
                                              const Slice& existing_value,
                                              std::string* /*new_value*/,
                                              bool* /*value_changed*/) const {
  // remove kv pairs that have empty values
  return existing_value.empty();
}

}  // namespace ROCKSDB_NAMESPACE
