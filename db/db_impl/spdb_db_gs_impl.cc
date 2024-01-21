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

#include <memory>

#include "memory/arena.h"
#include "db/db_impl/db_impl.h"
#include "db/range_del_aggregator.h"
#include "db/db_impl/spdb_gs_del_list.h"
#include "table/merging_iterator.h"
#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

namespace {
  void PrintFragmentedRangeDels(const std::string& title, FragmentedRangeTombstoneIterator* iter) {
  printf("%s - FragmentedRangeTombstoneIterator:\n", title.c_str());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    printf("{%s, %s, seq:%d}, ",
            ExtractUserKey(iter->key()).ToString().c_str(),
            iter->value().ToString().c_str(),
            (int)iter->seq());
  }
}

class FragmentedRangeTombstoneIteratorWrapper: public Iterator {
  public:
    FragmentedRangeTombstoneIteratorWrapper(std::unique_ptr<FragmentedRangeTombstoneIterator> wrapped_iter_ptr):
      wrapped_iter_ptr_(std::move(wrapped_iter_ptr)) {}

  bool Valid() const override {
    return (wrapped_iter_ptr_? wrapped_iter_ptr_->Valid() : false);
  }

  void SeekToFirst() override {
    if (wrapped_iter_ptr_) {
      wrapped_iter_ptr_->SeekToFirst();
    }
  }

  void SeekToLast()override {
    if (wrapped_iter_ptr_) {
      wrapped_iter_ptr_->SeekToLast();
    }
  }

  void Seek(const Slice& target) override {
    if (wrapped_iter_ptr_) {
      wrapped_iter_ptr_->Seek(target);
    }
  }

  void SeekForPrev(const Slice& /* target */) override {
    assert(0);
  }

  void Next() override {
    if (wrapped_iter_ptr_) {
      wrapped_iter_ptr_->Next();
    } else {
      assert(0);
    }

  }

  void Prev() override {
    assert(0);
  }

  Slice key() const override {
    if (wrapped_iter_ptr_) {
      return wrapped_iter_ptr_->key();
     } else {
      assert(0);
      return Slice();
     }
   }

  Slice value() const override {
    if (wrapped_iter_ptr_) {
      return wrapped_iter_ptr_->value();
     } else {
      assert(0);
      return Slice();
     }
   }

  Status status() const override {
    return (wrapped_iter_ptr_? wrapped_iter_ptr_->status() : Status::OK());
  }

  private:
    std::unique_ptr<FragmentedRangeTombstoneIterator> wrapped_iter_ptr_;
};

}

namespace spdb_gs {

Status ProcessLogLevel([[maybe_unused]] GlobalDelList* del_list,
                       InternalIterator* values_iter,
                       std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter,
                       std::string* curr_suk,
                       const Comparator* comparator) {
  assert(values_iter != nullptr);
  assert(del_list != nullptr);

  Status s;

  FragmentedRangeTombstoneIteratorWrapper range_del_iter_wrapper(std::move(range_del_iter));

  values_iter->SeekToFirst();
  range_del_iter_wrapper.SeekToFirst();

  while (values_iter->Valid() || range_del_iter_wrapper.Valid()) {
    if (values_iter->Valid()) {
      ParsedInternalKey values_parsed_ikey;
      ParseInternalKey(values_iter->key(), &values_parsed_ikey, true /* log_err_key */);      
      if (values_parsed_ikey.type == kTypeValue) {
        if (curr_suk->empty() || (comparator->Compare(values_parsed_ikey.user_key, *curr_suk))) {
          curr_suk->assign(values_parsed_ikey.user_key.data(), values_parsed_ikey.user_key.size());
          break;
        }
      }
      values_iter->Next();
    }
  }

  return s;
}

}  // namespace spdb_gs

Status DBImpl::GetSmallest( const ReadOptions& read_options,
                            ColumnFamilyHandle* column_family,
                            std::string* key,
                            std::string* /* value */) {                              
  assert(read_options.timestamp == nullptr);
  assert(read_options.snapshot == nullptr);
  assert(read_options.ignore_range_deletions == false);

  // TODO - Support snapshots
  SequenceNumber seq_num = kMaxSequenceNumber;

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();

  SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);

  // TODO - Figure out what to do about the Arena
  Arena arena;

  // ================
  // MUTABLE Memtable
  // ================
  // auto mem_iter = super_version->mem->NewIterator(read_options, arena.get());
  auto mem_iter = super_version->mem->NewIterator(read_options, &arena);

  Status s;
  auto range_del_iter = super_version->mem->NewRangeTombstoneIterator(
      read_options, seq_num, false /* immutable_memtable */);

  if ((range_del_iter != nullptr) && (range_del_iter->empty())) {
    PrintFragmentedRangeDels("DBImpl::GetSmallest - ", range_del_iter);
  }

  spdb_gs::GlobalDelList del_list(cfd->user_comparator());  
  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter_ptr(range_del_iter);
  ProcessLogLevel(&del_list, mem_iter,
                  std::move(range_del_iter_ptr),
                  key, cfd->user_comparator());

  mem_iter->~InternalIterator();

  CleanupSuperVersion(super_version);  

  if (key->empty()) {
    return Status::NotFound();
  }
  
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
