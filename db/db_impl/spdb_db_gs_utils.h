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

#pragma once

#include <string>

#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {
// Forward Declarations
struct RangeTombstone;
class FragmentedRangeTombstoneIterator;

namespace spdb_gs {

enum class ValueCategory { VALUE, MERGE_VALUE, DEL_KEY, OTHER, NONE };

ValueCategory GetValueCategoryOfKey(ValueType value_type);

enum class RelativePos { BEFORE, OVERLAP, AFTER };
enum class OverlapType {
  IDENTICAL,
  CONTAINS,
  CONTAINED,
  STARTS_BEFORE_ENDS_BEFORE,
  STARTS_AFTER_ENDS_AFTER,
  NONE
};

struct RelativePosInfo {
  RelativePos relative_pos;
  OverlapType overlap_type = OverlapType::NONE;

  RelativePosInfo(RelativePos _relative_pos) : relative_pos(_relative_pos) {}
  RelativePosInfo(RelativePos _relative_pos, OverlapType _overlap_type)
      : relative_pos(_relative_pos), overlap_type(_overlap_type) {}
};

void PrintFragmentedRangeDels(
    const std::string& title,
    ROCKSDB_NAMESPACE::FragmentedRangeTombstoneIterator* iter);

struct DelElement {
  std::string user_start_key;
  std::string user_end_key;

  // To construct a del-key
  DelElement(const Slice& _user_start_key)
      : user_start_key(_user_start_key.data(), _user_start_key.size()) {}

  // To construct a del-range
  DelElement(const Slice& _user_start_key, const Slice& _user_end_key)
      : user_start_key(_user_start_key.data(), _user_start_key.size()),
        user_end_key(_user_end_key.data(), _user_end_key.size()) {}

  bool IsRange() const { return (user_end_key.empty() == false); }
  bool IsDelKey() const { return (IsRange() == false); }

  bool operator==(const DelElement& other) const {
    return ((user_start_key == other.user_start_key) &&
            (user_end_key == other.user_end_key));
  }

  static bool LessThan(const DelElement& first, const DelElement& second,
                       const Comparator* comparator) {
    // Ordering based only on the start key
    return (comparator->Compare(first.user_start_key, second.user_start_key) <
            0);
  }

  std::string ToString() const {
    if (IsDelKey()) {
      return (std::string("{") + user_start_key + "}");
    } else {
      return (std::string("{") + user_start_key + ", " + user_end_key + "}");
    }
  }
};

RelativePos CompareRangeTsToUserKey(const RangeTombstone& range_ts,
                                    const Slice& user_key,
                                    const Comparator* comparator);

RelativePos CompareDelElemToUserKey(const DelElement& del_elem,
                                    const Slice& user_key,
                                    const Comparator* comparator);

RelativePos CompareDelElemToRangeTs(const DelElement& del_elem,
                                    const RangeTombstone& range_ts,
                                    const Comparator* comparator,
                                    OverlapType* overlap_type);

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
