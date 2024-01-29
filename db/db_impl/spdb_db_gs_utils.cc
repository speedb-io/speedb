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

#include "db/db_impl/spdb_db_gs_utils.h"

#include <stdio.h>

#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "rocksdb/comparator.h"

namespace ROCKSDB_NAMESPACE {
namespace spdb_gs {

void PrintFragmentedRangeDels(
    const std::string& title,
    ROCKSDB_NAMESPACE::FragmentedRangeTombstoneIterator* iter) {
  printf("%s - FragmentedRangeTombstoneIterator:\n", title.c_str());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    printf("{%s, %s, seq:%d}, ", ExtractUserKey(iter->key()).ToString().c_str(),
           iter->value().ToString().c_str(), (int)iter->seq());
  }
}

ValueCategory GetValueCategoryOfKey(ValueType value_type) {
  switch (value_type) {
    case kTypeValue:
      return ValueCategory::VALUE;

    case kTypeMerge:
      return ValueCategory::MERGE_VALUE;

    case kTypeDeletion:
      [[fallthrough]];
    case kTypeSingleDeletion:
      [[fallthrough]];
    case kTypeDeletionWithTimestamp:
      return ValueCategory::DEL_KEY;

    default:
      return ValueCategory::OTHER;
  }
}

namespace {
RelativePos ComparisonResultToRelativePos(int comparison_result) {
  if (comparison_result < 0) {
    return RelativePos::BEFORE;
  } else if (comparison_result == 0) {
    return RelativePos::OVERLAP;
  } else {
    return RelativePos::AFTER;
  }
}
}  // namespace

int CompareRangeTsToUserKeyInt(const RangeTombstone& range_ts,
                               const Slice& user_key,
                               const Comparator* comparator) {
  int result = 0;

  int user_key_vs_start_key =
      comparator->Compare(user_key, range_ts.start_key_);

  if (user_key_vs_start_key < 0) {
    // Range > user-key
    result = 1;
  } else {
    int user_key_vs_end_key = comparator->Compare(user_key, range_ts.end_key_);
    if (user_key_vs_end_key >= 0) {
      // Range < User-Key (The Range is half-open)
      result = -1;
    } else {
      result = 0;
    }
  }

  return result;
}

RelativePos CompareRangeTsToUserKey(const RangeTombstone& range_ts,
                                    const Slice& user_key,
                                    const Comparator* comparator) {
  return ComparisonResultToRelativePos(
      CompareRangeTsToUserKeyInt(range_ts, user_key, comparator));
}

RelativePos CompareDelElemToUserKey(const DelElement& del_elem,
                                    const Slice& user_key,
                                    const Comparator* comparator) {
  int result = 0;

  int user_key_vs_start_key =
      comparator->Compare(user_key, del_elem.user_start_key);

  // A del-elem may be a del-key => no end key
  if (del_elem.IsDelKey()) {
    result = -1 * user_key_vs_start_key;
  } else if (user_key_vs_start_key < 0) {
    // Del-Elem Range > user-key
    result = 1;
  } else {
    assert(del_elem.user_end_key.empty() == false);

    int user_key_vs_end_key =
        comparator->Compare(user_key, del_elem.user_end_key);
    if (user_key_vs_end_key >= 0) {
      // Del-Elem Range < User-Key (The Range is half-open)
      result = -1;
    } else {
      result = 0;
    }
  }

  return ComparisonResultToRelativePos(result);
}

RelativePos CompareDelElemToRangeTs(const DelElement& del_elem,
                                    const RangeTombstone& range_ts,
                                    const Comparator* comparator,
                                    OverlapType* overlap_type) {
  if (overlap_type != nullptr) {
    *overlap_type = OverlapType::NONE;
  }

  if (del_elem.IsDelKey()) {
    // Comparing a range to a del-key in the del-list
    int range_ts_vs_del_key = CompareRangeTsToUserKeyInt(
        range_ts, del_elem.user_start_key, comparator);
    auto relative_pos = ComparisonResultToRelativePos(-1 * range_ts_vs_del_key);
    if ((overlap_type != nullptr) && (relative_pos == RelativePos::OVERLAP)) {
      *overlap_type = OverlapType::CONTAINED;
    }
  }

  int range_ts_start_vs_del_elem_end =
      comparator->Compare(range_ts.start_key_, del_elem.user_end_key);
  if (range_ts_start_vs_del_elem_end >= 0) {
    return RelativePos::BEFORE;
  }

  int del_elem_start_vs_range_ts_end =
      comparator->Compare(del_elem.user_start_key, range_ts.end_key_);
  if (del_elem_start_vs_range_ts_end >= 0) {
    return RelativePos::AFTER;
  }

  if (overlap_type != nullptr) {
    int range_ts_start_vs_del_elem_start =
        comparator->Compare(range_ts.start_key_, del_elem.user_start_key);

    int range_ts_end_vs_del_elem_end =
        comparator->Compare(range_ts.start_key_, del_elem.user_start_key);

    if (range_ts_start_vs_del_elem_start < 0) {
      if (range_ts_end_vs_del_elem_end > 0) {
        *overlap_type = OverlapType::CONTAINED;
      } else {
        *overlap_type = OverlapType::STARTS_AFTER_ENDS_AFTER;
      }
    } else {
      if (range_ts_end_vs_del_elem_end <= 0) {
        // Contains may be identical. Not differentiating
        *overlap_type = OverlapType::CONTAINS;
      } else {
        *overlap_type = OverlapType::STARTS_BEFORE_ENDS_BEFORE;
      }
    }
  }

  return RelativePos::OVERLAP;
}

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
