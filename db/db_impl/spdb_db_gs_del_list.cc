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

#include "db/db_impl/spdb_db_gs_utils.h"

namespace ROCKSDB_NAMESPACE {
namespace spdb_gs {

GlobalDelList::GlobalDelList(const Comparator* comparator)
    : comparator_(comparator) {}

std::unique_ptr<GlobalDelList::Iterator> GlobalDelList::NewIterator() {
  return std::make_unique<Iterator>(*this);
}

void GlobalDelList::InsertBefore(Iterator& pos, const DelElement& del_elem) {
  del_list_.insert(pos.del_list_iter_, del_elem);
}

void GlobalDelList::MergeWith(Iterator& pos, const DelElement& del_elem) {
  MergeWithInternal(pos.del_list_iter_, del_elem);
}

void GlobalDelList::MergeWithInternal(std::list<DelElement>::iterator pos,
                                      const DelElement& del_elem) {
  assert(pos != del_list_.end());

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
    return;
  }

  const auto& containing_del_elem = trim_iter.key();
  auto containing_start_vs_upper_bound =
      comparator_->Compare(containing_del_elem.user_start_key, upper_bound);

  if (containing_start_vs_upper_bound < 0) {
    ReplaceWith(trim_iter,
                DelElement(containing_del_elem.user_start_key, upper_bound));
  }
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
}

void GlobalDelList::Iterator::SeekForward(const Slice& seek_start_key) {
  if (glbl_del_list_.del_list_.empty()) {
    return;
  }

  auto CompareDelElems = [this](const DelElement& first,
                                const DelElement& second) {
    return DelElement::LessThan(first, second,
                                this->glbl_del_list_.comparator_);
  };

  std::list<DelElement>::iterator seek_start_pos =
      glbl_del_list_.del_list_.end();

  if (Valid()) {
    // Seek key must be greater than current
    assert(glbl_del_list_.comparator_->Compare(
               seek_start_key, del_list_iter_->user_start_key) > 0);
    // Start from current
    seek_start_pos = del_list_iter_;
  } else {
    // Invalid => start from the beginning
    seek_start_pos = glbl_del_list_.del_list_.begin();
  }

  DelElement seek_del_elem(seek_start_key);
  del_list_iter_ =
      std::lower_bound(seek_start_pos, glbl_del_list_.del_list_.end(),
                       seek_del_elem, CompareDelElems);

  if (del_list_iter_ != glbl_del_list_.del_list_.begin()) {
    // We are positioned at the first del-elem that is >= seek_start_key (by
    // del-elem start). If the previous element contains (not before)
    // seek_start_key, we will postion the iter on that del-elem.
    auto prev_iter = std::prev(del_list_iter_, 1);
    auto prev_del_elem_vs_seek_key = CompareDelElemToUserKey(
        *prev_iter, seek_start_key, glbl_del_list_.comparator_);
    assert(prev_del_elem_vs_seek_key != RelativePos::AFTER);

    if (prev_del_elem_vs_seek_key == RelativePos::OVERLAP) {
      del_list_iter_ = prev_iter;
    }
  }
  del_list_prev_iter_ = glbl_del_list_.del_list_.end();
}

void GlobalDelList::Iterator::Next() {
  assert(Valid());
  del_list_prev_iter_ = del_list_iter_;
  ++del_list_iter_;

  if (del_list_iter_ != glbl_del_list_.del_list_.end()) {
    if (glbl_del_list_.comparator_->Compare(del_list_prev_iter_->user_end_key,
                                            del_list_iter_->user_start_key) >
        0) {
      glbl_del_list_.MergeWithInternal(del_list_iter_, *del_list_prev_iter_);

      glbl_del_list_.del_list_.erase(del_list_prev_iter_);
      // The prev iterator is now invalid
      del_list_prev_iter_ = glbl_del_list_.del_list_.end();
    }
  }
}

const DelElement& GlobalDelList::Iterator::key() const {
  assert(Valid());
  return (*del_list_iter_);
}

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
