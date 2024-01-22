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

#include "include/rocksdb/comparator.h"
#include "include/rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
namespace spdb_gs {

class GlobalDelList {
 public:
  struct DelElement {
    std::string user_start_key;
    std::string user_end_key;

    // To construct a del-key
    explicit DelElement(const std::string& _user_start_key)
        : user_start_key(_user_start_key) {}

    // To construct a del-range
    DelElement(const std::string& _user_start_key,
               const std::string& _user_end_key)
        : user_start_key(_user_start_key), user_end_key(_user_end_key) {}

    bool IsRange() const { return (user_end_key.empty() == false); }
    bool IsDelKey() const { return (IsRange() == false); }

    bool operator==(const DelElement& other) const {
      return ((user_start_key == other.user_start_key) &&
              (user_end_key == other.user_end_key));
    }

    std::string ToString() const {
      if (IsDelKey()) {
        return (std::string("{") + user_start_key + "}");
      } else {
        return (std::string("{") + user_start_key + ", " + user_end_key + "}");
      }
    }
  };

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

    // No copying
    Iterator(const Iterator&) = delete;
    Iterator(Iterator&&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    bool Valid() const;

    void SeekToFirst();
    void Seek(const Slice& user_start_key);

    void Next();

    const DelElement& key() const;

   private:
    GlobalDelList& glbl_del_list_;
    std::list<DelElement>::iterator del_list_iter_;
    std::list<DelElement>::iterator del_list_prev_iter_;

    friend class GlobalDelList;
  };

  bool Empty() const { return del_list_.empty(); }
  size_t Size() const { return del_list_.size(); }

  std::unique_ptr<Iterator> NewIterator();

  // Insert del_elem into the list before pos.
  // It is assumed (and not validated) that del_elem is inserted in-order:
  // del_elem must precede the pointed element. Otherwise, the del-list will
  // be corrupt, violating its assumptions.
  void InsertBefore(Iterator& pos, const DelElement& del_elem);

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

  // Remove all elements > user_start_key.
  // If start_pos is != nullptr, the list is trimmed from start_pos onwards.
  // The assumption is that the end of the del_elem pointed by start_pos is
  // after user_start_key.
  void TrimList(const Slice& user_start_key, Iterator* start_pos = nullptr);

  std::string ToString() const;

 private:
  void MergeWithInternal(std::list<DelElement>::iterator pos,
                         const DelElement& del_elem);

 private:
  const Comparator* comparator_ = nullptr;
  std::list<DelElement> del_list_;
};

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
