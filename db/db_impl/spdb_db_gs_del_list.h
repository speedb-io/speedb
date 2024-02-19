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

#include <list>
#include <memory>
#include <string>
#include <atomic>

#include "db/db_impl/spdb_db_gs_utils.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
namespace spdb_gs {

class GlobalDelList {
 public:
  GlobalDelList(const Comparator* comparator);

  GlobalDelList(const GlobalDelList&) = delete;
  GlobalDelList operator=(const GlobalDelList&) = delete;
  GlobalDelList(GlobalDelList&&) = delete;

  class Iterator {
   public:
    // TODO - Make private and understand why GlobalDelList can't access
    // although it's a friend class
    Iterator(GlobalDelList& del_list);

    // No copying / assignment
    Iterator(const Iterator&) = delete;
    Iterator(Iterator&&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    bool Valid() const;

    void SeekToFirst();
    void Seek(const Slice& target);
    
    // Move the iterator forward, until the first del-elem that either
    // contains or is after user_start_key (by the del-elem's start key).
    // The del-element is searched from the current iterator's position until
    // the end of the list.
    //
    // Pre-Requisite: If the iterator is Valid() => seek_start_key must be >=
    // iter->key().user_start_key.
    void SeekForward(const Slice& seek_start_key);

    void Next();

    const DelElement& key() const;
    
   private:
    GlobalDelList& glbl_del_list_;
    std::list<DelElement>::iterator del_list_iter_;
    std::list<DelElement>::iterator del_list_prev_iter_;

    size_t min_dist_from_begin_ = 0U;
    friend class GlobalDelList;
  };

  bool Empty() const { return del_list_.empty(); }
  size_t Size() const { return del_list_.size(); }

  std::unique_ptr<Iterator> NewIterator();

  // Insert del_elem into the list BEFORE pos.
  // It is assumed (and not validated) that del_elem is inserted in-order:
  // del_elem must precede the pointed element. Otherwise, the del-list will
  // be corrupt, violating its assumptions.
  void InsertBefore(Iterator& pos, const DelElement& del_elem);

  void InsertBeforeAndSetIterOnInserted(Iterator& pos,
                                        const DelElement& del_elem);

  // Merge del_elem with the del_elem pointed to by the iterator at pos.
  // The del_elem and the pointed del_elem must be overlapping.
  // Note that the resulting del elem may overlap del-elements that are
  // yet to be iterated. When moving to the next del element (if pos is not the
  // last element), the need to merge will be evaluated. If there is a need, the
  // previous element will be merged with the next one, and the previous will be
  // removed frmo the list.
  //
  // Pre-Requisites: The iterator must be Valid()
  void MergeWith(Iterator& pos, const DelElement& del_elem);

  // Replace the element pointed to by pos by del_elem.
  //
  // Pre-Requisites: The iterator must be Valid()
  void ReplaceWith(Iterator& pos, const DelElement& del_elem);

  // Remove all elements >= upper_bound (based on del-elem's start keys).
  //
  // If the upper_bound falls inside a del-element [start, end), then this
  // del-element will be replaced by [start, upper_bound).
  void Trim(const Slice& upper_bound);

  Slice GetUpperBound() const {
    return (upper_bound_.empty() == false) ? upper_bound_ : Slice();
  }

  bool Valid() const;

  std::string ToString() const;

  static std::atomic<uint64_t> num_seek_forwards;
  static std::atomic<uint64_t> num_found_in_binary_search;

 private:
  void MergeWithInternal(std::list<DelElement>::iterator pos,
                         const DelElement& del_elem);

 private:
  std::list<DelElement> del_list_;
  const Comparator* comparator_ = nullptr;
  Slice upper_bound_;
};

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
