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

    bool IsRange() const { return (user_end_key.empty() == false); }

    bool IsDelKey() const { return (IsRange() == false); }
  };

 public:
  GlobalDelList(const Comparator* comparator) : comparator_(comparator){};

  GlobalDelList(const GlobalDelList&) = delete;
  GlobalDelList operator=(const GlobalDelList&) = delete;
  GlobalDelList(GlobalDelList&&) = delete;

  class Iterator {
   public:
    Iterator(const std::list<DelElement>& del_list);

    // No copying
    Iterator(const Iterator&) = delete;
    Iterator(Iterator&&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    void Valid() const;

    void SeekToFirst();
    void Seek(const Slice& user_start_key);

    void Next();

    const DelElement& key() const;

   private:
    const GlobalDelList& del_list_;
    std::list<DelElement>::const_iterator del_list_iter_;
  };

  std::unique_ptr<Iterator> NewIterator();

  void InsertBefore(const Iterator& pos, const DelElement& element);

  // Merge element with the element pointed to by the iterator at pos.
  // The element and the pointed element must be overlapping.
  void MergeWith(const Iterator& pos, const DelElement& element);

  // Remove all elements > user_start_key.
  // If start_pos is != nullptr, the list is trimmed from start_pos onwards.
  // The assumption is that the end of the element pointed by start_pos is
  // after user_start_key.
  void TrimList(const Slice& user_start_key,
                const Iterator* start_pos = nullptr);

 private:
  const Comparator* comparator_ = nullptr;
};

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
