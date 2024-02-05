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
#include <string>

#include "db/db_impl/db_impl.h"
#include "db/db_impl/spdb_db_gs_del_list.h"
#include "db/db_impl/spdb_db_gs_utils.h"
#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "db/range_tombstone_fragmenter.h"
#include "memory/arena.h"
#include "rocksdb/status.h"

extern bool gs_debug_prints;

namespace ROCKSDB_NAMESPACE {
namespace spdb_gs {

namespace {

class InternalIteratorWrapper : public Iterator {
 public:
  InternalIteratorWrapper(std::unique_ptr<InternalIterator> wrapped_iter_ptr,
                          const Comparator* comparator,
                          const Slice& upper_bound)
      : wrapped_iter_ptr_(std::move(wrapped_iter_ptr)),
        comparator_(comparator),
        upper_bound_(upper_bound) {
    assert(wrapped_iter_ptr_);
    assert(comparator != nullptr);
  }

  ~InternalIteratorWrapper() {
    wrapped_iter_ptr_.get()->~InternalIterator();
    wrapped_iter_ptr_.release();
  }

  bool Valid() const override { return valid_; }

  bool HasUpperBound() const { return (upper_bound_.empty() == false); }

  void SetUpperBound(const Slice& upper_bound) {
    upper_bound_ = upper_bound;
    UpdateValidity();
  }

  const Slice& GetUpperBound() const {
    assert(HasUpperBound());
    return upper_bound_;
  }

  void SeekToFirst() override {
    wrapped_iter_ptr_->SeekToFirst();
    UpdateValidity();
  }

  void SeekToLast() override {
    wrapped_iter_ptr_->SeekToLast();
    UpdateValidity();
  }

  void Seek(const Slice& target) override {
    // TODO - Handle Sequence number correctly
    LookupKey lookup_key(target, kMaxSequenceNumber);
    auto target_ikey = lookup_key.internal_key();
    wrapped_iter_ptr_->Seek(target_ikey);
    UpdateValidity();
  }

  void Next() override {
    assert(Valid());
    wrapped_iter_ptr_->Next();
    UpdateValidity();
  }

  void SeekForPrev(const Slice& /* target */) override {
    assert(0);
    Invalidate();
  }

  void Prev() override { assert(0); }

  Slice key() const override {
    assert(Valid());
    return wrapped_iter_ptr_->key();
  }

  Slice value() const override {
    assert(Valid());
    return wrapped_iter_ptr_->value();
  }

  Status status() const override { return wrapped_iter_ptr_->status(); }

  void Invalidate() { valid_ = false; }

 private:
  void UpdateValidity() {
    valid_ = wrapped_iter_ptr_->Valid();
    if (valid_ && HasUpperBound()) {
      auto curr_value_vs_upper_bound =
          comparator_->Compare(wrapped_iter_ptr_->key(), upper_bound_);
      // The upper bound is the csk => excluding the csk from the iteration.
      valid_ = (curr_value_vs_upper_bound < 0);
    }
  }

 private:
  std::unique_ptr<InternalIterator> wrapped_iter_ptr_;
  const Comparator* comparator_ = nullptr;
  Slice upper_bound_;
  bool valid_ = false;
};

class FragmentedRangeTombstoneIteratorWrapper : public Iterator {
 public:
  FragmentedRangeTombstoneIteratorWrapper(
      std::unique_ptr<FragmentedRangeTombstoneIterator> wrapped_iter_ptr,
      const Comparator* comparator, const Slice& upper_bound)
      : wrapped_iter_ptr_(std::move(wrapped_iter_ptr)),
        comparator_(comparator),
        upper_bound_(upper_bound) {}

  bool Valid() const override { return valid_; }

  bool HasUpperBound() const { return (upper_bound_.empty() == false); }

  void SetUpperBound(const Slice& upper_bound) {
    upper_bound_ = upper_bound;
    UpdateValidity();
  }

  const Slice& GetUpperBound() const {
    assert(HasUpperBound());
    return upper_bound_;
  }

  void SeekToFirst() override {
    if (wrapped_iter_ptr_) {
      wrapped_iter_ptr_->SeekToFirst();
      UpdateValidity();
    }
  }

  void SeekToLast() override {
    if (wrapped_iter_ptr_) {
      wrapped_iter_ptr_->SeekToLast();
      UpdateValidity();
    }
  }

  void Seek(const Slice& target) override {
    if (wrapped_iter_ptr_) {
      wrapped_iter_ptr_->Seek(target);
      UpdateValidity();
    }
  }

  void Next() override {
    if (wrapped_iter_ptr_) {
      assert(Valid());
      wrapped_iter_ptr_->Next();
      UpdateValidity();
    } else {
      assert(0);
    }
  }

  void SeekForPrev(const Slice& /* target */) override { assert(0); }

  void Prev() override { assert(0); }

  Slice key() const override {
    if (wrapped_iter_ptr_) {
      assert(Valid());
      return wrapped_iter_ptr_->key();
    } else {
      return Slice();
    }
  }

  Slice value() const override {
    if (wrapped_iter_ptr_) {
      assert(Valid());
      return wrapped_iter_ptr_->value();
    } else {
      return Slice();
    }
  }

  Status status() const override {
    return (wrapped_iter_ptr_ ? wrapped_iter_ptr_->status() : Status::OK());
  }

  RangeTombstone Tombstone() const {
    if (wrapped_iter_ptr_) {
      assert(Valid());
      auto curr_range_ts = wrapped_iter_ptr_->Tombstone();
      if (HasUpperBound() == false) {
        return curr_range_ts;
      }

      assert(comparator_->Compare(curr_range_ts.start_key_, upper_bound_) < 0);
      auto curr_range_end_vs_upper_bound =
          comparator_->Compare(curr_range_ts.end_key_, upper_bound_);
      if (curr_range_end_vs_upper_bound <= 0) {
        return curr_range_ts;
      }

      // curr range extends beyond the upper bound, return a range that ends at
      // the upper bound (exclusive)
      return RangeTombstone(curr_range_ts.start_key_, upper_bound_,
                            curr_range_ts.seq_);
    } else {
      assert(0);
      return RangeTombstone();
    }
  }

  void Invalidate() {
    if (wrapped_iter_ptr_) {
      wrapped_iter_ptr_->Invalidate();
    }
  }

 private:
  void UpdateValidity() {
    if (wrapped_iter_ptr_ == nullptr) {
      valid_ = false;
    } else {
      valid_ = wrapped_iter_ptr_->Valid();
      if (valid_ && HasUpperBound()) {
        auto curr_range_start_vs_upper_bound = comparator_->Compare(
            wrapped_iter_ptr_->start_key(), GetUpperBound());
        // The upper bound is exclusive for ranges;
        // A range that starts at the upper bound is invalid
        if (curr_range_start_vs_upper_bound >= 0) {
          valid_ = false;
        }
      }
    }
  }

 private:
  std::unique_ptr<FragmentedRangeTombstoneIterator> wrapped_iter_ptr_;
  const Comparator* comparator_ = nullptr;
  Slice upper_bound_;
  bool valid_ = false;
};

struct GlobalContext {
  ReadOptions mutable_read_options;
  SequenceNumber seq_num = kMaxSequenceNumber;
  GlobalDelList* del_list = nullptr;
  Slice target;
  std::string csk;
  const Comparator* comparator = nullptr;
  std::shared_ptr<Logger> logger;

  std::unique_ptr<GlobalDelList::Iterator> del_list_iter;
};

struct LevelContext {
  std::unique_ptr<InternalIteratorWrapper> values_iter;
  std::unique_ptr<FragmentedRangeTombstoneIteratorWrapper> range_del_iter;

  // std::string prev_del_key;

  // These are valid only when applicable
  ParsedInternalKey values_parsed_ikey;
  ValueCategory value_category = ValueCategory::NONE;

  bool new_csk_found_in_level = false;
};

void UpdateCSK(GlobalContext& gc, LevelContext& lc) {
  auto new_csk = lc.values_parsed_ikey.user_key;

  if (gs_debug_prints) {
    printf("UpdateCSK curr:%s, new:%s\n",
           ((gc.csk.empty() == false) ? gc.csk.c_str() : "NONE"),
           new_csk.ToString().c_str());
  }

  gc.csk.assign(new_csk.data(), new_csk.size());
  gc.del_list->Trim(new_csk);
  lc.range_del_iter->SetUpperBound(new_csk);

  // Not setting the upper bound of the values-iter since it is currently
  // positioned on it

  lc.new_csk_found_in_level = true;
}

void ProcessCurrRangeTsVsDelList(GlobalContext& gc, LevelContext& lc) {
  assert(lc.range_del_iter->Valid());

  auto range_ts = lc.range_del_iter->Tombstone();

  // values iter is Invalid, DR Iter is valid => Add to global del-list and
  // complete level processing
  if (gc.del_list_iter->Valid()) {
    auto& del_elem = gc.del_list_iter->key();

    RelativePos overlap_start_rel_pos{RelativePos::NONE};
    RelativePos overlap_end_rel_pos{RelativePos::NONE};
    auto del_list_vs_range_ts = CompareDelElemToRangeTs(
        gc.del_list_iter->key(), range_ts, gc.comparator,
        &overlap_start_rel_pos, &overlap_end_rel_pos);

    switch (del_list_vs_range_ts) {
      case RelativePos::BEFORE:
        gc.del_list_iter->SeekForward(range_ts.start_key_);
        break;

      case RelativePos::AFTER:
        gc.del_list->InsertBefore(
            *gc.del_list_iter,
            DelElement(range_ts.start_key_, range_ts.end_key_));
        lc.range_del_iter->Next();
        break;

      case RelativePos::OVERLAP: {
        bool del_elem_starts_at_or_before_range_ts =
            (overlap_start_rel_pos == RelativePos::BEFORE) ||
            (overlap_start_rel_pos == RelativePos::OVERLAP);
        bool del_elem_ends_before_range_ts =
            (overlap_end_rel_pos == RelativePos::BEFORE);

        if (del_elem_starts_at_or_before_range_ts) {
          if (del_elem_ends_before_range_ts) {
            gc.del_list->ReplaceWith(
                *gc.del_list_iter,
                DelElement(del_elem.user_start_key, range_ts.end_key_));
            gc.del_list_iter->SeekForward(range_ts.end_key_);
          } else {
            // Del-elem contains range-ts => Nothing to do
          }
        } else {
          if (del_elem_ends_before_range_ts) {
            // Range-ts contains del-elem
            gc.del_list->ReplaceWith(
                *gc.del_list_iter,
                DelElement(range_ts.start_key_, range_ts.end_key_));
            gc.del_list_iter->SeekForward(range_ts.end_key_);
          } else {
            // del-elem start after range ts but ends after
            gc.del_list->ReplaceWith(
                *gc.del_list_iter,
                DelElement(range_ts.start_key_, del_elem.user_end_key));
            lc.range_del_iter->Seek(del_elem.user_end_key);
          }
        }
      } break;

      default:
        assert(0);
        break;
    }
  } else {
    // del list exhausted
    gc.del_list->InsertBefore(
        *gc.del_list_iter, DelElement(range_ts.start_key_, range_ts.end_key_));
    lc.range_del_iter->Next();
  }
}

bool ProcessCurrValuesIterVsDelList(GlobalContext& gc, LevelContext& lc) {
  auto del_list_vs_values_iter_key = RelativePos::AFTER;

  if (gc.del_list_iter->Valid()) {
    del_list_vs_values_iter_key = CompareDelElemToUserKey(
        gc.del_list_iter->key(), lc.values_parsed_ikey.user_key, gc.comparator,
        nullptr /* overlap_start_rel_pos */, nullptr /* overlap_end_rel_pos */);
  }

  switch (del_list_vs_values_iter_key) {
    case RelativePos::BEFORE:
      gc.del_list_iter->SeekForward(lc.values_parsed_ikey.user_key);
      break;

    case RelativePos::AFTER:
      if ((lc.value_category == ValueCategory::VALUE) ||
          (lc.value_category == ValueCategory::MERGE_VALUE)) {
        UpdateCSK(gc, lc);
      } else if (lc.value_category == ValueCategory::DEL_KEY) {
        gc.del_list->InsertBeforeAndSetIterOnInserted(
            *gc.del_list_iter, DelElement(lc.values_parsed_ikey.user_key));
        lc.values_iter->Next();
      }
      break;

    case RelativePos::OVERLAP:
      // The key is covered by the Del-Elem => it is irrelevant (as all of the
      // range covered)
      if (gc.del_list_iter->key().IsRange()) {
        lc.values_iter->Seek(gc.del_list_iter->key().user_end_key);
      } else {
        lc.values_iter->Next();
      }
      break;

    default:
      assert(0);
      return false;
      break;
  }

  return lc.new_csk_found_in_level;
}

Status ProcessLogLevel(GlobalContext& gc, LevelContext& lc) {
  if (gc.target.empty()) {
    gc.del_list_iter->SeekToFirst();
    lc.values_iter->SeekToFirst();
    lc.range_del_iter->SeekToFirst();
  } else {
    gc.del_list_iter->Seek(gc.target);
    lc.values_iter->Seek(gc.target);
    lc.range_del_iter->Seek(gc.target);
  }

  while ((lc.new_csk_found_in_level == false) &&
         (lc.values_iter->Valid() || lc.range_del_iter->Valid())) {
    if (lc.values_iter->Valid() == false) {
      // Range ts iter is Valid() values_iter is Invalid()
      ProcessCurrRangeTsVsDelList(gc, lc);
      continue;
    }

    // values_iter is valid
    [[maybe_unused]] auto parsing_status = ParseInternalKey(
        lc.values_iter->key(), &lc.values_parsed_ikey, true /* log_err_key */);
    assert(parsing_status.ok());
    lc.value_category = GetValueCategoryOfKey(lc.values_parsed_ikey.type);

    if (lc.range_del_iter->Valid() == false) {
      auto was_new_csk_found = ProcessCurrValuesIterVsDelList(gc, lc);
      if (was_new_csk_found) {
        if (gs_debug_prints)
          printf("Processing Level Ended, new csk was found\n");
        return Status::OK();
      } else {
        continue;
      }
    }

    if (lc.value_category == ValueCategory::OTHER) {
      lc.values_iter->Next();
      continue;
    }

    auto range_ts = lc.range_del_iter->Tombstone();
    auto range_ts_vs_values_iter_key = CompareRangeTsToUserKey(
        range_ts, lc.values_parsed_ikey.user_key, gc.comparator,
        nullptr /* overlap_start_rel_pos */, nullptr /* overlap_end_rel_pos */);

    switch (range_ts_vs_values_iter_key) {
      case RelativePos::BEFORE:
        ProcessCurrRangeTsVsDelList(gc, lc);
        break;

      case RelativePos::AFTER:
        ProcessCurrValuesIterVsDelList(gc, lc);
        break;

      case RelativePos::OVERLAP: {
        if (lc.value_category == ValueCategory::DEL_KEY) {
          // The del-key is covered by the range-ts => ignore it
          lc.values_iter->Next();
          continue;
        }

        assert((lc.value_category == ValueCategory::VALUE) ||
               (lc.value_category == ValueCategory::MERGE_VALUE));

        assert(range_ts.seq_ != lc.values_parsed_ikey.sequence);
        if (range_ts.seq_ < lc.values_parsed_ikey.sequence) {
          // DR Is Older
          ProcessCurrValuesIterVsDelList(gc, lc);
        } else {
          // DR Is newer => the value / merge-value is covered by the range-ts
          // => irrelevant
          lc.values_iter->Next();
        }
      } break;

      default:
        assert(0);
        // Bug in code
        return Status::Aborted();
        break;
    }
  }

  if (gs_debug_prints)
    printf("Processing Level Ended, was new csk was found:%d\n",
           lc.new_csk_found_in_level);

  return Status::OK();
}

Status ProcessMutableMemtable(SuperVersion* super_version, GlobalContext& gc,
                              Arena& arena) {
  LevelContext lc;

  std::unique_ptr<InternalIterator> wrapped_values_iter(
      super_version->mem->NewIterator(gc.mutable_read_options, &arena));
  lc.values_iter.reset(new InternalIteratorWrapper(
      std::move(wrapped_values_iter), gc.comparator, gc.csk));

  auto range_del_iter = super_version->mem->NewRangeTombstoneIterator(
      gc.mutable_read_options, gc.seq_num, false /* immutable_memtable */);
  std::unique_ptr<FragmentedRangeTombstoneIterator> wrapped_range_del_iter;
  if (range_del_iter != nullptr) {
    wrapped_range_del_iter.reset(std::move(range_del_iter));
  }
  lc.range_del_iter.reset(new FragmentedRangeTombstoneIteratorWrapper(
      std::move(wrapped_range_del_iter), gc.comparator, gc.csk));

  if (gs_debug_prints) printf("Processing Mutable Table\n");
  return ProcessLogLevel(gc, lc);
}

Status ProcessImmutableMemtables(SuperVersion* super_version, GlobalContext& gc,
                                 Arena& arena) {
  auto iters =
      super_version->imm->GetIterators(gc.mutable_read_options, &arena);

  if (gs_debug_prints)
    printf("Processing Immutable Memtables. Num Memtables:%zu\n", iters.size());

  auto i = 1;
  for (auto& memtbl_iters : iters) {
    LevelContext lc;
    lc.values_iter.reset(new InternalIteratorWrapper(
        std::move(memtbl_iters.memtbl_iter), gc.comparator, gc.csk));

    std::unique_ptr<FragmentedRangeTombstoneIterator> wrapped_range_del_iter;
    if (memtbl_iters.range_ts_iter.get() != nullptr) {
      wrapped_range_del_iter.reset(memtbl_iters.range_ts_iter.release());
    }
    lc.range_del_iter.reset(new FragmentedRangeTombstoneIteratorWrapper(
        std::move(wrapped_range_del_iter), gc.comparator, gc.csk));

    if (gs_debug_prints) printf("Processing Immutable Memtable #%d\n", i);
    auto status = ProcessLogLevel(gc, lc);
    if (status.ok() == false) {
      return status;
    }
    ++i;
  }
  return Status::OK();
}

Status ProcessLevel0Files(SuperVersion* super_version, GlobalContext& gc,
                          const FileOptions& file_options, Arena& arena) {
  constexpr int level0 = 0;

  if (super_version->current->storage_info()->IsLevelEmpty(level0)) {
    if (gs_debug_prints) printf("Level-0 Is Empty\n");
    return Status::OK();
  }

  // TOOD - Handle allow_unprepared_value!!!!
  auto iters = super_version->current->GetLevel0Iterators(
      gc.mutable_read_options, file_options, false /* allow_unprepared_value */,
      &arena);

  if (gs_debug_prints)
    printf("Processing Level-0 Files. Num Files:%zu\n", iters.size());

  auto i = 1;
  for (auto& file_iters : iters) {
    LevelContext lc;
    lc.values_iter.reset(new InternalIteratorWrapper(
        std::move(file_iters.table_iter), gc.comparator, gc.csk));

    std::unique_ptr<FragmentedRangeTombstoneIterator> wrapped_range_del_iter;
    if (file_iters.range_ts_iter.get() != nullptr) {
      wrapped_range_del_iter.reset(file_iters.range_ts_iter.release());
    }
    lc.range_del_iter.reset(new FragmentedRangeTombstoneIteratorWrapper(
        std::move(wrapped_range_del_iter), gc.comparator, gc.csk));

    if (gs_debug_prints) printf("Processing Level-0 File #%d\n", i);
    auto status = ProcessLogLevel(gc, lc);
    if (status.ok() == false) {
      return status;
    }
    ++i;
  }
  return Status::OK();
}

}  // unnamed namespace
}  // namespace spdb_gs

Status DBImpl::GetSmallestAtOrAfter(const ReadOptions& read_options,
                                    ColumnFamilyHandle* column_family,
                                    const Slice& target, std::string* key,
                                    std::string* /* value */) {
  assert(read_options.timestamp == nullptr);
  assert(read_options.snapshot == nullptr);
  assert(read_options.ignore_range_deletions == false);
  assert(key != nullptr);

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();

  SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);

  spdb_gs::GlobalDelList del_list(cfd->user_comparator());

  spdb_gs::GlobalContext gc;
  gc.mutable_read_options = read_options;
  // TODO - Support snapshots
  gc.seq_num = kMaxSequenceNumber;
  gc.del_list = &del_list;
  if (target.empty() == false) {
    gc.target = target;
  }
  gc.csk.clear();
  gc.comparator = cfd->user_comparator();
  gc.logger = immutable_db_options_.info_log;

  // TODO - Create this once for all levels rather than recreate per level
  gc.del_list_iter.reset(gc.del_list->NewIterator().release());

  // TODO - User the CSK to set the upper bound of applicable iterators

  // TODO - Figure out what to do about the Arena
  Arena arena;

  auto status = ProcessMutableMemtable(super_version, gc, arena);

  if (status.ok()) {
    status = ProcessImmutableMemtables(super_version, gc, arena);
  }

  if (status.ok()) {
    status = ProcessLevel0Files(super_version, gc, file_options_, arena);
  }

  CleanupSuperVersion(super_version);

  if (gc.csk.empty()) {
    *key = "";
    return Status::NotFound();
  }

  // Verify that the found key >= user's key
  assert(target.empty() || (gc.comparator->Compare(target, gc.csk) <= 0));
  *key = gc.csk;

  return status;
}

Status DBImpl::GetSmallest(const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, std::string* key,
                           std::string* value) {
  std::string target = "";
  return GetSmallestAtOrAfter(read_options, column_family, target, key, value);
}

}  // namespace ROCKSDB_NAMESPACE
