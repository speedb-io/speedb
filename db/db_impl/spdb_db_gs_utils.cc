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

RelativePos ReverseRelativePos(RelativePos curr_relative_pos) {
  switch (curr_relative_pos) {
    case RelativePos::BEFORE:
      return RelativePos::AFTER;
    case RelativePos::AFTER:
      return RelativePos::BEFORE;
    case RelativePos::OVERLAP:
      return RelativePos::OVERLAP;
    case RelativePos::NONE:
      return RelativePos::NONE;
    default:
      assert(0);
      return RelativePos::NONE;
  }
}
}  // unnamed namespace

RelativePos CompareRangeToUserKey(const Slice& range_start_key,
                                  const Slice& range_end_key,
                                  const Slice& user_key,
                                  const Comparator* comparator,
                                  RelativePos* overlap_start_rel_pos,
                                  RelativePos* overlap_end_rel_pos) {
  RelativePos result = RelativePos::NONE;
  if (overlap_start_rel_pos != nullptr) {
    *overlap_start_rel_pos = RelativePos::NONE;
  }
  if (overlap_end_rel_pos != nullptr) {
    *overlap_end_rel_pos = RelativePos::NONE;
  }

  int range_start_key_vs_user_key =
      comparator->Compare(range_start_key, user_key);

  if (range_start_key_vs_user_key > 0) {
    // Range > user-key
    result = RelativePos::AFTER;

  } else if (range_start_key_vs_user_key == 0) {
    // User key is on range's start
    result = RelativePos::OVERLAP;

    if (overlap_start_rel_pos != nullptr) {
      *overlap_start_rel_pos = RelativePos::OVERLAP;
    }
    if (overlap_end_rel_pos != nullptr) {
      // Assuming the range is NOT empty
      // The range [a, b) actually contains a single key (a) but we can't know
      // that
      *overlap_end_rel_pos = RelativePos::BEFORE;
    }

  } else {
    // Range starts before user-key, now check the range end vs the user-key
    auto range_end_key_vs_user_key =
        comparator->Compare(range_end_key, user_key);

    if (range_end_key_vs_user_key <= 0) {
      // Range < User-Key (The Range is half-open)
      // The range [a, b) is BEFORE the user-key b
      result = RelativePos::BEFORE;
    } else {
      result = RelativePos::OVERLAP;

      // Range starts before user key
      if (overlap_start_rel_pos != nullptr) {
        *overlap_start_rel_pos = RelativePos::BEFORE;
      }
      if (overlap_end_rel_pos != nullptr) {
        *overlap_end_rel_pos = RelativePos::AFTER;
      }
    }
  }

  if (result == RelativePos::OVERLAP) {
    assert((overlap_start_rel_pos == nullptr) ||
           (*overlap_end_rel_pos != RelativePos::NONE));
    assert((overlap_end_rel_pos == nullptr) ||
           (*overlap_end_rel_pos != RelativePos::NONE));
  }

  return result;
}

RelativePos CompareRangeTsToUserKey(const RangeTombstone& range_ts,
                                    const Slice& user_key,
                                    const Comparator* comparator,
                                    RelativePos* overlap_start_rel_pos,
                                    RelativePos* overlap_end_rel_pos) {
  return CompareRangeToUserKey(range_ts.start_key_, range_ts.end_key_, user_key,
                               comparator, overlap_start_rel_pos,
                               overlap_end_rel_pos);
}

RelativePos CompareDelElemToUserKey(const DelElement& del_elem,
                                    const Slice& user_key,
                                    const Comparator* comparator,
                                    RelativePos* overlap_start_rel_pos,
                                    RelativePos* overlap_end_rel_pos) {
  if (overlap_start_rel_pos != nullptr) {
    *overlap_start_rel_pos = RelativePos::NONE;
  }
  if (overlap_end_rel_pos != nullptr) {
    *overlap_end_rel_pos = RelativePos::NONE;
  }

  // A del-elem may be a del-key => no end key
  if (del_elem.IsDelKey()) {
    // Actually comparing 2 keys
    int del_elem_start_key_vs_user_key =
        comparator->Compare(del_elem.user_start_key, user_key);
    auto result = ComparisonResultToRelativePos(del_elem_start_key_vs_user_key);

    if (result == RelativePos::OVERLAP) {
      if (overlap_start_rel_pos != nullptr) {
        *overlap_start_rel_pos = RelativePos::OVERLAP;
      }
      if (overlap_end_rel_pos != nullptr) {
        *overlap_end_rel_pos = RelativePos::OVERLAP;
      }
    }
    return result;
  } else {
    return CompareRangeToUserKey(del_elem.user_start_key, del_elem.user_end_key,
                                 user_key, comparator, overlap_start_rel_pos,
                                 overlap_end_rel_pos);
  }
}

RelativePos CompareDelElemToRangeTs(const DelElement& del_elem,
                                    const RangeTombstone& range_ts,
                                    const Comparator* comparator,
                                    RelativePos* overlap_start_rel_pos,
                                    RelativePos* overlap_end_rel_pos) {
  if (overlap_start_rel_pos != nullptr) {
    *overlap_start_rel_pos = RelativePos::NONE;
  }
  if (overlap_end_rel_pos != nullptr) {
    *overlap_end_rel_pos = RelativePos::NONE;
  }

  if (del_elem.IsDelKey()) {
    auto range_ts_vs_del_key =
        CompareRangeTsToUserKey(range_ts, del_elem.user_start_key, comparator,
                                overlap_start_rel_pos, overlap_end_rel_pos);

    if (overlap_start_rel_pos != nullptr) {
      *overlap_start_rel_pos = ReverseRelativePos(*overlap_start_rel_pos);
    }
    if (overlap_end_rel_pos != nullptr) {
      *overlap_end_rel_pos = ReverseRelativePos(*overlap_end_rel_pos);
    }

    return ReverseRelativePos(range_ts_vs_del_key);
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

  // They overlap => set the overlap info if requested
  if (overlap_start_rel_pos != nullptr) {
    int del_elem_start_vs_range_ts_start =
        comparator->Compare(del_elem.user_start_key, range_ts.start_key_);
    *overlap_start_rel_pos =
        ComparisonResultToRelativePos(del_elem_start_vs_range_ts_start);
  }

  if (overlap_end_rel_pos != nullptr) {
    int del_elem_end_vs_range_ts_end =
        comparator->Compare(del_elem.user_end_key, range_ts.end_key_);
    *overlap_end_rel_pos =
        ComparisonResultToRelativePos(del_elem_end_vs_range_ts_end);
  }

  return RelativePos::OVERLAP;
}

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
