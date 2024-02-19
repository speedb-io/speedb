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

#include "db/db_impl/spdb_db_gs_del_list.h"

#include <algorithm>
#include <functional>
#include <cmath>

#include "db/db_impl/spdb_db_gs_utils.h"

extern bool gs_optimize_seek_forward;
bool gs_optimize_seek_forward = true;

namespace ROCKSDB_NAMESPACE {
namespace spdb_gs {

GlobalDelList::GlobalDelList(const Comparator* comparator)
    : comparator_(comparator) {}

std::unique_ptr<GlobalDelList::Iterator> GlobalDelList::NewIterator() {
  return std::make_unique<Iterator>(*this);
}

void GlobalDelList::InsertBefore(Iterator& pos, const DelElement& del_elem) {
  del_list_.insert(pos.del_list_iter_, del_elem);
  // assert(Valid());
}

void GlobalDelList::InsertBeforeAndSetIterOnInserted(
    Iterator& pos, const DelElement& del_elem) {
  pos.del_list_iter_ = del_list_.insert(pos.del_list_iter_, del_elem);
  assert(Valid());
}

void GlobalDelList::MergeWith(Iterator& pos, const DelElement& del_elem) {
  MergeWithInternal(pos.del_list_iter_, del_elem);
  assert(Valid());
}

void GlobalDelList::MergeWithInternal(std::list<DelElement>::iterator pos,
                                      const DelElement& del_elem) {
  assert(pos != del_list_.end());
  assert(del_elem.IsWithinUpperBound(upper_bound_, comparator_));

  if (pos->IsDelKey()) {
    // If both are del-keys, nothing to do
    // If, however, merging a range-ts with a del-list del-key, just replace the
    // del-key with the merged range
    if (del_elem.IsRange()) {
      *pos = del_elem;
    }
  } else {
    // Both are ranges, merge them
    auto MinMaxCompare = [this](const std::string& first,
                                const std::string& second) {
      return (this->comparator_->Compare(first, second) <= 0);
    };

    pos->user_start_key = std::min<std::string>(
        pos->user_start_key, del_elem.user_start_key, MinMaxCompare);

    pos->user_end_key = std::max<std::string>(
        pos->user_end_key, del_elem.user_end_key, MinMaxCompare);
  }
  assert(Valid());
}

// Replace the element pointed to by pos by del_elem.
//
// Pre-Requisites: The iterator must be Valid()
void GlobalDelList::ReplaceWith(Iterator& pos, const DelElement& del_elem) {
  assert(pos.Valid());
  *pos.del_list_iter_ = del_elem;
}

void GlobalDelList::Trim(const Slice& upper_bound) {
  Iterator trim_iter(*this);
  trim_iter.SeekForward(upper_bound);

  if (trim_iter.Valid() == false) {
    upper_bound_ = upper_bound;
    assert(Valid());
    return;
  }

  const auto& containing_del_elem = trim_iter.key();
  auto containing_start_vs_upper_bound =
      comparator_->Compare(containing_del_elem.user_start_key, upper_bound);

  if (containing_start_vs_upper_bound < 0) {
    ReplaceWith(trim_iter,
                DelElement(containing_del_elem.user_start_key, upper_bound));
    trim_iter.Next();
  }

  del_list_.erase(trim_iter.del_list_iter_, del_list_.end());

  upper_bound_ = upper_bound;
  assert(Valid());
}

bool GlobalDelList::Valid() const {
  if (del_list_.empty() || upper_bound_.empty()) {
    return true;
  }

  const Slice& last_del_elem_range_end = del_list_.back().RangeEnd();
  return (comparator_->Compare(last_del_elem_range_end, upper_bound_) <= 0);
}

std::string GlobalDelList::ToString() const {
  if (Empty()) {
    return "<<< Empty Del List >>>";
  }

  std::string str_rep =
      std::string("Del List (") + std::to_string(Size()) + " Els):";
  for (const auto& del_elem : del_list_) {
    str_rep += del_elem.ToString() + ", ";
  }

  return str_rep;
}

// ====================================================================================
//                              Iterator
// ====================================================================================
GlobalDelList::Iterator::Iterator(GlobalDelList& glbl_del_list)
    : glbl_del_list_(glbl_del_list),
      del_list_iter_(glbl_del_list.del_list_.end()),
      del_list_prev_iter_(glbl_del_list.del_list_.end()) {
  // Do Nothing
}

bool GlobalDelList::Iterator::Valid() const {
  return (del_list_iter_ != glbl_del_list_.del_list_.end());
}

void GlobalDelList::Iterator::SeekToFirst() {
  del_list_iter_ = glbl_del_list_.del_list_.begin();
  min_dist_from_begin_ = 0U;
}

void GlobalDelList::Iterator::Seek(const Slice& target) {
  del_list_iter_ = glbl_del_list_.del_list_.end();
  SeekForward(target);
}

  // if (gs_optimize_seek_forward) {
  //   // TODO - Should be the ceil(log2(N))
  //   auto max_num_lower_bound_comparisons = std::log2(glbl_del_list_.Size());
  //   auto temp_iter = del_list_iter_;
  //   auto num_skipped = 0U;
  //   while ((temp_iter != glbl_del_list_.del_list_.end()) && (num_skipped < max_num_lower_bound_comparisons)) {
  //     ++temp_iter;
  //     ++num_skipped;
  //   }
  //   if (temp_iter == glbl_del_list_.del_list_.end()) {
  //     --temp_iter;
  //   }
  //   auto seek_key_vs_temp_del_elem_end = glbl_del_list_.comparator_->Compare(seek_start_key, temp_iter->RangeEnd());
  //   if (seek_key_vs_temp_del_elem_end < 0) {
  //     // temp_iter points at a del-elem that ends after the seek key => no point looking past it
  //     lower_bound_end_pos = temp_iter;
  //   } else {
  //     // temp_iter points at a del-elem that ends before the seek key => start looking from the del-elem following temp_iter
  //     seek_start_pos = temp_iter;
  //     ++seek_start_pos;
  //   }
  // }

  // if (del_list_iter_ != glbl_del_list_.del_list_.begin()) {
  //   // We are positioned at the first del-elem that is >= seek_start_key (by
  //   // del-elem start). If the previous element contains (not before)
  //   // seek_start_key, we will postion the iter on that del-elem.
  //   auto prev_iter = std::prev(del_list_iter_, 1);
  //   auto prev_del_elem_vs_seek_key = CompareDelElemToUserKey(
  //       *prev_iter, seek_start_key, glbl_del_list_.comparator_,
  //       nullptr /* overlap_start_rel_pos */, nullptr /* overlap_end_rel_pos */);
  //   assert(prev_del_elem_vs_seek_key != RelativePos::AFTER);

  //   if (prev_del_elem_vs_seek_key == RelativePos::OVERLAP) {
  //     del_list_iter_ = prev_iter;
  //   }
  // }

void GlobalDelList::Iterator::SeekForward(const Slice& seek_start_key) {
  if (glbl_del_list_.del_list_.empty()) {
    return;
  }

  auto CompareDelElems = [this](const DelElement& first,
                                const DelElement& second) {
    const Comparator* comparator = this->glbl_del_list_.comparator_;

    if (first.IsDelKey() && second.IsDelKey()) {
      return (comparator->Compare(first.user_start_key, second.user_start_key) < 0);
    } else if (first.IsRange() && second.IsDelKey()) {
      return (comparator->Compare(first.user_end_key, second.user_start_key) <= 0);
    } else if (first.IsDelKey() && second.IsRange()) {
      return (comparator->Compare(first.user_start_key, second.user_start_key) < 0);      
    } else {
      // Both are Ranges
      return (comparator->Compare(first.user_end_key, second.user_start_key) <= 0);
    }
  };

  std::list<DelElement>::iterator seek_pos = glbl_del_list_.del_list_.end();
  if (Valid()) {
    // Seek key must be greater than current
    assert(glbl_del_list_.comparator_->Compare(
               seek_start_key, del_list_iter_->user_start_key) > 0);
    // Start from current
    seek_pos = del_list_iter_;
  } else {
    // Invalid => start from the beginning
    seek_pos = glbl_del_list_.del_list_.begin();
  }

  del_list_iter_ = glbl_del_list_.del_list_.end();
  del_list_prev_iter_ = glbl_del_list_.del_list_.end();

  auto num_scanned = 0U;
  while ((num_scanned < 3) && (seek_pos != glbl_del_list_.del_list_.end())) {
    if (seek_pos->IsDelKey()) {
      auto seek_pos_key_vs_seek_key = glbl_del_list_.comparator_->Compare(seek_pos->user_start_key, seek_start_key);
      if (seek_pos_key_vs_seek_key >= 0) {
        del_list_iter_ = seek_pos;
        return;
      }
    } else {
      auto seek_pos_end_range_vs_seek_key = glbl_del_list_.comparator_->Compare(seek_pos->user_end_key, seek_start_key);
      if (seek_pos_end_range_vs_seek_key > 0) {
        del_list_iter_ = seek_pos;
        return;
      }
    }
    ++num_scanned;
    ++seek_pos;
  }
  
  DelElement seek_del_elem(seek_start_key);
  del_list_iter_ =
      std::lower_bound(seek_pos, glbl_del_list_.del_list_.end(),
                       seek_del_elem, CompareDelElems);

  del_list_prev_iter_ = glbl_del_list_.del_list_.end();
}

void GlobalDelList::Iterator::Next() {
  assert(Valid());
  del_list_prev_iter_ = del_list_iter_;
  ++del_list_iter_;
  ++min_dist_from_begin_;

  if (del_list_iter_ != glbl_del_list_.del_list_.end()) {
    if (glbl_del_list_.comparator_->Compare(del_list_prev_iter_->user_end_key,
                                            del_list_iter_->user_start_key) >
        0) {
      glbl_del_list_.MergeWithInternal(del_list_iter_, *del_list_prev_iter_);

      glbl_del_list_.del_list_.erase(del_list_prev_iter_);
      // The prev iterator is now invalid
      del_list_prev_iter_ = glbl_del_list_.del_list_.end();
      --min_dist_from_begin_;
    }
  }
}

const DelElement& GlobalDelList::Iterator::key() const {
  assert(Valid());
  return (*del_list_iter_);
}

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
