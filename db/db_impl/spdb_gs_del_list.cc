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

#include "db/db_impl/spdb_gs_del_list.h"

namespace ROCKSDB_NAMESPACE {
namespace spdb_gs {

std::unique_ptr<GlobalDelList::Iterator> GlobalDelList::NewIterator() {
  return std::make_unique<Iterator>(del_list_);
}

void GlobalDelList::InsertBefore(Iterator& pos, const DelElement& del_elem) {
  del_list_.insert(pos.del_list_iter_, del_elem);
}

void GlobalDelList::MergeWith(Iterator& pos, const DelElement& del_elem) {
  if (pos.del_list_iter_->IsDelKey()) {
    // If both are del-keys, nothing to do
    // If, however, merging a range-ts with a del-list del-key, just replace the
    // del-key with the merged range
    if (del_elem.IsRange()) {
      *(pos.del_list_iter_) = del_elem;
    }
  } else {
    // Both are ranges, merge them
    auto MinMaxCompare = [this](const std::string& first,
                                const std::string& second) {
      return (this->comparator_->Compare(first, second) <= 0);
    };

    pos.del_list_iter_->user_start_key =
        std::min<std::string>(pos.del_list_iter_->user_start_key,
                              del_elem.user_start_key, MinMaxCompare);

    pos.del_list_iter_->user_end_key = std::max<std::string>(
        pos.del_list_iter_->user_end_key, del_elem.user_end_key, MinMaxCompare);
  }
}

void GlobalDelList::TrimList(const Slice& /* user_start_key */,
                             Iterator* /* start_pos */) {}

GlobalDelList::Iterator::Iterator(std::list<DelElement>& del_list)
    : del_list_(del_list), del_list_iter_(del_list.end()) {
  // Do Nothing
}

bool GlobalDelList::Iterator::Valid() const {
  return (del_list_iter_ != del_list_.end());
}

void GlobalDelList::Iterator::SeekToFirst() {
  del_list_iter_ = del_list_.begin();
}

void GlobalDelList::Iterator::Seek(const Slice& /* user_start_key */) {
  assert(0);
}

void GlobalDelList::Iterator::Next() {
  assert(Valid());
  ++del_list_iter_;
}

const GlobalDelList::DelElement& GlobalDelList::Iterator::key() const {
  assert(Valid());
  return (*del_list_iter_);
}

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
