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
#include "util/stop_watch.h"

extern bool gs_debug_prints;
bool gs_debug_prints = false;

extern bool gs_validate_iters_progress;
bool gs_validate_iters_progress = false;

extern bool gs_report_iters_progress;
bool gs_report_iters_progress = false;

namespace ROCKSDB_NAMESPACE {
namespace spdb_gs {

namespace {

class LevelFilesItersMngr {
  public:
   LevelFilesItersMngr(SuperVersion* super_version,
                       const InternalKeyComparator* icomparator,
                       const ReadOptions& read_options,
                       const FileOptions& file_options, Arena* arena,
                       const LevelFilesBrief& level_files_brief,
                       const Slice& lower_bound, const Slice& upper_bound)
       : super_version_(super_version),
         icomparator_(icomparator),
         read_options_(read_options),
         file_options_(file_options),
         arena_(arena),
         level_files_brief_(level_files_brief),
         files_iters_(level_files_brief.num_files),
         lower_bound_(lower_bound),
         upper_bound_(upper_bound),
         first_file_idx_within_bounds_(NumFiles()),
         last_file_idx_within_bounds_(NumFiles()) {
     assert(lower_bound.empty() || upper_bound.empty() ||
            icomparator_->user_comparator()->Compare(lower_bound,
                                                     upper_bound) <= 0);

     if (lower_bound_.empty() == false) {
       first_file_idx_within_bounds_ =
           FindFileForUserKey(lower_bound_, NumFiles());
     } else {
       first_file_idx_within_bounds_ = 0;
     }

     if (first_file_idx_within_bounds_ < NumFiles()) {
       curr_internal_iter_idx_ = first_file_idx_within_bounds_ - 1;
       curr_range_ts_iter_idx_ = first_file_idx_within_bounds_ - 1;

       if (upper_bound_.empty() == false) {
         last_file_idx_within_bounds_ =
             FindFileForUserKey(upper_bound_, NumFiles() - 1);
       } else {
         last_file_idx_within_bounds_ = NumFiles() - 1;
       }

       invalid_file_idx_ = last_file_idx_within_bounds_ + 1;
       empty_ = false;
     } else {
       last_file_idx_within_bounds_ = NumFiles();
       invalid_file_idx_ = NumFiles();
       curr_internal_iter_idx_ = NumFiles();
       curr_range_ts_iter_idx_ = NumFiles();
       empty_ = true;
     }

     assert(first_file_idx_within_bounds_ <= last_file_idx_within_bounds_);
   }

   ~LevelFilesItersMngr() {
     for (auto i = 0U; i < files_iters_.size(); ++i) {
       if (files_iters_[i] != nullptr) {
         auto& file_iters = files_iters_[i];
         if (file_iters->table_iter != nullptr) {
           file_iters->table_iter->~InternalIterator();
           file_iters->table_iter = nullptr;
         }

         delete file_iters->range_ts_iter;
         file_iters->range_ts_iter = nullptr;
       }
     }
   }

   bool IsEmpty() const { return empty_; }

   bool HasNextInternalIter() const {
     return (curr_internal_iter_idx_ < last_file_idx_within_bounds_);
   }

    InternalIterator* NextInternalIter() {
      assert(HasNextInternalIter());

      UpdateInternalIterIdx(curr_internal_iter_idx_ + 1);
      return files_iters_[curr_internal_iter_idx_]->table_iter;
    }

    bool HasNextRangeTsIter() const {
      return (curr_range_ts_iter_idx_ < last_file_idx_within_bounds_);
    }

    TruncatedRangeDelIterator* NextRangeTsIter() {
      assert(HasNextRangeTsIter());

      UpdateRangeTsIterIdx(curr_range_ts_iter_idx_ + 1);
      return files_iters_[curr_range_ts_iter_idx_]->range_ts_iter;
    }

    InternalIterator* AdvanceInternalIterToFileContainingKey(const Slice& key) {
      if (IsEmpty()) {
        return nullptr;
      }

      auto new_iter_idx =
          FindFileForwardContainingKey(key, curr_internal_iter_idx_);
      if (new_iter_idx != curr_internal_iter_idx_) {
        UpdateInternalIterIdx(new_iter_idx);
      }

      if (IsFileIdxWithinUpperBound(curr_internal_iter_idx_)) {
        return files_iters_[curr_internal_iter_idx_]->table_iter;
      } else {
        return nullptr;
      }
    }

    TruncatedRangeDelIterator* AdvanceRangeTsIterToFileContainingKey(
        const Slice& key) {
      if (IsEmpty()) {
        return nullptr;
      }

      auto new_iter_idx =
          FindFileForwardContainingKey(key, curr_range_ts_iter_idx_);
      if (new_iter_idx != curr_range_ts_iter_idx_) {
        UpdateRangeTsIterIdx(new_iter_idx);
      }

      if (IsFileIdxWithinUpperBound(curr_range_ts_iter_idx_)) {
        return files_iters_[curr_range_ts_iter_idx_]->range_ts_iter;
      } else {
        return nullptr;
      }
    }

  private:
   bool IsKeyWithinFileBoundaries(int file_idx, const Slice& user_key) const {
     assert(IsFileIdxWithinBounds(file_idx));

     // TODO - Handle Snapshots / Sequence
     InternalKey ikey;
     ikey.SetMinPossibleForUserKey(user_key);
     return IsKeyWithinFileBounaries(*icomparator_, level_files_brief_,
                                     file_idx, ikey.Encode());
   }

   void UpdateInternalIterIdx(int new_idx) {
     assert(new_idx > curr_internal_iter_idx_);

     CleanupInternalIter(curr_internal_iter_idx_);
     if (IsFileIdxWithinUpperBound(new_idx)) {
       CreateFileItersIfNecesary(new_idx);
     }
     curr_internal_iter_idx_ = new_idx;
   }

   void UpdateRangeTsIterIdx(int new_idx) {
     assert(new_idx > curr_range_ts_iter_idx_);

     if (IsFileIdxWithinBounds(curr_range_ts_iter_idx_)) {
       CleanupRangeTsIter(curr_range_ts_iter_idx_);
     }

     if (IsFileIdxWithinUpperBound(new_idx)) {
       CreateFileItersIfNecesary(new_idx);
     }
     curr_range_ts_iter_idx_ = new_idx;
   }

    void CreateFileItersIfNecesary(int file_idx) {
      assert(IsFileIdxWithinBounds(file_idx));

      if (DoFileItersExist(file_idx) == false) {
        auto file_iters = std::make_unique<Version::IteratorPair>(
            super_version_->current->GetFileIters(
                read_options_, file_options_, true /* allow_unprepared_value */,
                arena_, level_files_brief_.files[file_idx]));
        files_iters_[file_idx].reset(file_iters.release());
      }
    }

    int FindFileForwardContainingKey(const Slice& key, int curr_iter_idx) {
      assert(IsFileIdxWithinUpperBound(curr_iter_idx));

      if ((curr_iter_idx >= first_file_idx_within_bounds_) &&
          IsKeyWithinFileBoundaries(curr_iter_idx, key)) {
        assert(DoFileItersExist(curr_iter_idx));
        // Make sure we are not stuck endlessly on the same key
        // TODO - Update this to handle snapshots / seq-nums
        assert(icomparator_->user_comparator()->Compare(
                   files_iters_[curr_iter_idx]->table_iter->key(), key) < 0);
        return curr_iter_idx;
      }

      auto new_file_idx = FindFileForUserKey(key, InvalidFileIdx());
      assert(curr_iter_idx < new_file_idx);
      return new_file_idx;
    }

    void CleanupInternalIter(int file_idx) {
      if (IsFileIdxWithinBounds(file_idx) == false) {
        return;
      }

      auto& file_iters = files_iters_[file_idx];

      if (file_iters->table_iter != nullptr) {
        file_iters->table_iter->~InternalIterator();
        file_iters->table_iter = nullptr;
      }
    }

    void CleanupRangeTsIter(int file_idx) {
      auto& file_iters = files_iters_[file_idx];
      delete file_iters->range_ts_iter;
      file_iters->range_ts_iter = nullptr;
    }

    bool DoFileItersExist(int file_idx) const {
      assert(IsFileIdxWithinBounds(file_idx));
      // table_iter is never nullptr when created for a file (range_ts_iter may
      // be)
      return (files_iters_[file_idx] != nullptr);
    }

    int FindFileForUserKey(const Slice& user_key, int invalid_file_idx) const {
      assert(user_key.empty() == false);

      // Find the leftmost possible internal key for user_key
      InternalKey ikey;
      ikey.SetMinPossibleForUserKey(user_key);
      auto file_idx =
          FindFile(*icomparator_, level_files_brief_, ikey.Encode());
      if (IsFileIdxValid(file_idx) == false) {
        file_idx = invalid_file_idx;
      }
      return file_idx;
    }

    int InvalidFileIdx() const { return invalid_file_idx_; }

    bool IsFileIdxValid(int file_idx) const {
      return ((file_idx >= 0) && (file_idx < NumFiles()));
    }

    int NumFiles() const { return static_cast<int>(files_iters_.size()); }

    bool IsFileIdxWithinUpperBound(int file_idx) const {
      return (file_idx <= last_file_idx_within_bounds_);
    }

    bool IsFileIdxWithinBounds(int file_idx) const {
      return ((file_idx >= first_file_idx_within_bounds_) &&
              (file_idx <= last_file_idx_within_bounds_));
    }

  private:
    SuperVersion* super_version_ = nullptr;
    const InternalKeyComparator* icomparator_ = nullptr;
    const ReadOptions& read_options_;
    const FileOptions& file_options_;
    Arena* arena_ = nullptr;
    const LevelFilesBrief& level_files_brief_;

    std::vector<std::unique_ptr<Version::IteratorPair>> files_iters_;
    Slice lower_bound_;
    Slice upper_bound_;

    bool empty_ = true;
    int first_file_idx_within_bounds_ = -1;
    int last_file_idx_within_bounds_ = -1;
    int invalid_file_idx_ = -1;
    int curr_internal_iter_idx_ = -1;
    int curr_range_ts_iter_idx_ = -1;
};

std::string RangeTsToString(const RangeTombstone& range_ts) {
  if (range_ts.start_key_.empty()) {
    return "Empty Range-TS";
  }

  std::string result;
  result += "{" + range_ts.start_key_.ToString() + ", ";
  result += range_ts.start_key_.ToString() + "} ";
  result += "[" + std::to_string(range_ts.seq_) + "]";

  return result;
}

bool AreRangeTssEqual(const RangeTombstone& first, const RangeTombstone& second) {
  return ((first.start_key_ == second.start_key_) && (first.end_key_ == second.end_key_) && (first.seq_ == second.seq_));
}

class InternalIteratorWrapperBase {
 public:
  virtual ~InternalIteratorWrapperBase() = default;

  virtual bool Valid() const = 0;

  virtual bool HasUpperBound() const = 0;
  virtual const Slice& GetUpperBound() const = 0;

  virtual void SeekToFirst() = 0;
  virtual void SeekForward(const Slice& target) = 0;
  virtual void Next() = 0;

  virtual Slice key() const = 0;
  virtual Slice value() const = 0;

  virtual std::string ToString() const = 0;
};

class InternalIteratorWrapper: public InternalIteratorWrapperBase {
 public:
  InternalIteratorWrapper(InternalIterator* wrapped_iter_ptr,
                          const Comparator* comparator,
                          const Slice& upper_bound, bool is_iter_owner)
      : wrapped_iter_(wrapped_iter_ptr),
        comparator_(comparator),
        upper_bound_(upper_bound),
        iter_owner_(is_iter_owner) {
    assert(wrapped_iter_ != nullptr);
    assert(comparator != nullptr);
  }

  ~InternalIteratorWrapper() {
    if (iter_owner_) {
      wrapped_iter_->~InternalIterator();
    }
    wrapped_iter_ = nullptr;
  }

  bool Valid() const override { return valid_; }

  bool HasUpperBound() const override { return (upper_bound_.empty() == false); }

  const Slice& GetUpperBound() const override {
    assert(HasUpperBound());
    return upper_bound_;
  }

  void SeekToFirst()  override{
    wrapped_iter_->SeekToFirst();
    UpdateValidity();
    ReportProgress("SeekToFirst"); 
  }

  void SeekForward(const Slice& target) override {
    // TODO - Handle Sequence number correctly
    LookupKey lookup_key(target, kMaxSequenceNumber);
    auto target_ikey = lookup_key.internal_key();
    wrapped_iter_->Seek(target_ikey);
    UpdateValidity();
    ReportProgress("Seek");
  }

  void Next() override {
    assert(Valid());
    wrapped_iter_->Next();
    UpdateValidity();
    ReportProgress("Next"); 
  }

  Slice key() const override {
    assert(Valid());
    return wrapped_iter_->key();
  }

  Slice value() const override {
    assert(Valid());
    return wrapped_iter_->value();
  }

  std::string ToString() const override {
    if (Valid() == false) {
      return "Invalid";
    }
    return key().ToString();
  }

  InternalIterator* Wrapped() { return wrapped_iter_; }

  void ReportProgress(const std::string& progress_str) {
    if (gs_report_iters_progress) {
      std::cout << "Values-Iter: " << progress_str << ":" << ToString() << '\n';
    }
  }

 private:
  void UpdateValidity() {
    valid_ = wrapped_iter_->Valid();
    if (valid_ && HasUpperBound()) {
      auto curr_value_vs_upper_bound =
          comparator_->Compare(wrapped_iter_->key(), upper_bound_);
      // The upper bound is the csk => excluding the csk from the iteration.
      valid_ = (curr_value_vs_upper_bound < 0);
    }
  }

 private:
  InternalIterator* wrapped_iter_ = nullptr;
  const Comparator* comparator_ = nullptr;
  Slice upper_bound_;
  bool iter_owner_ = false;
  bool valid_ = false;
};

class InternalLevelIteratorWrapper : public InternalIteratorWrapperBase {
 public:
  InternalLevelIteratorWrapper(LevelFilesItersMngr& level_iters_mngr,
                               const Comparator* comparator,
                               const Slice& upper_bound)
      : level_iters_mngr_(level_iters_mngr),
        comparator_(comparator),
        upper_bound_(upper_bound) {
    assert(comparator != nullptr);
  }

  bool Valid() const override { return valid_; }

  bool HasUpperBound() const override {
    return (upper_bound_.empty() == false);
  }

  const Slice& GetUpperBound() const override {
    assert(HasUpperBound());
    return upper_bound_;
  }

  void SeekToFirst() override {
    assert(file_iter_wrapper_ == nullptr);
    SkipToFirstAvailableValue();
    UpdateValidity();
  }

  void SeekForward(const Slice& target) override {
    if (level_iters_mngr_.IsEmpty()) {
      return;
    }

    auto new_internal_iter =
        level_iters_mngr_.AdvanceInternalIterToFileContainingKey(target);

    if (new_internal_iter == nullptr) {
      // A null internal iterator means we have exhausted all values within this
      // level
      assert(level_iters_mngr_.HasNextInternalIter() == false);
      CleanupFileIterWrapper();
      UpdateValidity();
      return;
    }

    if ((file_iter_wrapper_ == nullptr) ||
        (new_internal_iter != file_iter_wrapper_->Wrapped())) {
      // The target key is in a new file
      InitFileIterWrapper(new_internal_iter);
    }

    // The target is within the current file's boundaries.
    // Try to position the iterator on the correct key within this file.
    file_iter_wrapper_->SeekForward(target);

    if (file_iter_wrapper_->Valid() == false) {
      SkipToFirstAvailableValue();
    }

    UpdateValidity();
  }

  void Next() override {
    assert(Valid());
    file_iter_wrapper_->Next();
    if (file_iter_wrapper_->Valid() == false) {
      SkipToFirstAvailableValue();
      UpdateValidity();
    }
  }

  Slice key() const override {
    assert(Valid());
    return file_iter_wrapper_->key();
  }

  Slice value() const override {
    assert(Valid());
    return file_iter_wrapper_->value();
  }

  std::string ToString() const override {
    if (Valid() == false) {
      return "Invalid";
    }
    return file_iter_wrapper_->ToString();
  }

 private:
  void InitFileIterWrapper(InternalIterator* wrapped_iter) {
    assert(wrapped_iter != nullptr);
    file_iter_wrapper_.reset(new InternalIteratorWrapper(
        wrapped_iter, comparator_, upper_bound_, false /* is_iter_owner */));
  }

  void CleanupFileIterWrapper() { file_iter_wrapper_.reset(); }

  void UpdateValidity() {
    valid_ = (file_iter_wrapper_ != nullptr) && (file_iter_wrapper_->Valid());
  }

  void SkipToFirstAvailableValue() {
    while (level_iters_mngr_.HasNextInternalIter() &&
           ((file_iter_wrapper_ == nullptr) ||
            (file_iter_wrapper_->Valid() == false))) {
      auto next_internal_iter = level_iters_mngr_.NextInternalIter();
      if (next_internal_iter != nullptr) {
        InitFileIterWrapper(next_internal_iter);
        file_iter_wrapper_->SeekToFirst();
      } else {
        CleanupFileIterWrapper();
      }
    }
  }

 private:
  LevelFilesItersMngr& level_iters_mngr_;
  std::unique_ptr<InternalIteratorWrapper> file_iter_wrapper_;
  const Comparator* comparator_ = nullptr;
  Slice upper_bound_;
  bool valid_ = false;
};

class TruncatedRangeDelIteratorWrapperBase {
 public:
  virtual ~TruncatedRangeDelIteratorWrapperBase() {}

  virtual bool Valid() const = 0;

  virtual bool HasUpperBound() const = 0;
  virtual void SetUpperBound(const Slice& upper_bound) = 0;
  virtual const Slice& GetUpperBound() const = 0;

  virtual void SeekToFirst() = 0;
  virtual void SeekForward(const Slice& target) = 0;
  virtual void Next() = 0;

  virtual RangeTombstone Tombstone() const = 0;

  virtual Slice key() const = 0;

  virtual std::string ToString() const = 0;
};

class TruncatedRangeDelIteratorWrapper
    : public TruncatedRangeDelIteratorWrapperBase {
 public:
  TruncatedRangeDelIteratorWrapper(TruncatedRangeDelIterator* wrapped_iter_ptr,
                                   const Comparator* comparator,
                                   const Slice& upper_bound, bool is_iter_owner)
      : wrapped_iter_(wrapped_iter_ptr),
        comparator_(comparator),
        upper_bound_(upper_bound),
        iter_owner_(is_iter_owner) {}

  ~TruncatedRangeDelIteratorWrapper() {
    if (iter_owner_) {
      delete wrapped_iter_;
    }
    wrapped_iter_ = nullptr;
  }

  bool Valid() const override { return valid_; }

  bool HasUpperBound() const override { return (upper_bound_.empty() == false); }

  void SetUpperBound(const Slice& upper_bound) override {
    upper_bound_ = upper_bound;
    UpdateValidity();
  }

  const Slice& GetUpperBound() const override {
    assert(HasUpperBound());
    return upper_bound_;
  }

  void SeekToFirst() override {
    if (wrapped_iter_) {
      wrapped_iter_->SeekToFirst();
      UpdateValidity();
      ReportProgress("SeekToFirst");
    }
  }

  void SeekForward(const Slice& target) override {
    if (wrapped_iter_) {
      wrapped_iter_->Seek(target);
      UpdateValidity();
      ReportProgress("Seek");
    }
  }

  void Next() override {
    if (wrapped_iter_) {
      assert(Valid());
      wrapped_iter_->Next();
      UpdateValidity();
      ReportProgress("Next");
    } else {
      assert(0);
    }
  }

  RangeTombstone Tombstone() const override {
    if (wrapped_iter_) {
      assert(Valid());
      auto curr_range_ts = wrapped_iter_->Tombstone();
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

  Slice key() const override {
    assert(Valid());
    return wrapped_iter_->start_key().user_key;
  }

  std::string ToString() const override {
    if (Valid() == false) {
      return "Invalid";
    }
    return RangeTsToString(Tombstone());
  }

  TruncatedRangeDelIterator* Wrapped() { return wrapped_iter_; }

  void ReportProgress(const std::string& progress_str) {
    if (gs_report_iters_progress) {
      std::cout << "Range-TS-Iter: " << progress_str << ":" << ToString() << '\n';
    }    
  }

 private:
  void UpdateValidity() {
    if (wrapped_iter_ == nullptr) {
      valid_ = false;
    } else {
      valid_ = wrapped_iter_->Valid();
      if (valid_ && HasUpperBound()) {
        auto curr_range_start_vs_upper_bound = comparator_->Compare(
            key(), GetUpperBound());
        // The upper bound is exclusive for ranges;
        // A range that starts at the upper bound is invalid
        if (curr_range_start_vs_upper_bound >= 0) {
          valid_ = false;
        }
      }
    }
  }

 private:
  TruncatedRangeDelIterator* wrapped_iter_ = nullptr;
  const Comparator* comparator_ = nullptr;
  Slice upper_bound_;
  bool iter_owner_ = false;
  bool valid_ = false;
};

class TruncatedRangeDelLevelIteratorWrapper
    : public TruncatedRangeDelIteratorWrapperBase {
 public:
  TruncatedRangeDelLevelIteratorWrapper(LevelFilesItersMngr& level_iters_mngr,
                                        const Comparator* comparator,
                                        const Slice& upper_bound)
      : level_iters_mngr_(level_iters_mngr),
        comparator_(comparator),
        upper_bound_(upper_bound) {
    assert(comparator != nullptr);
  }

  ~TruncatedRangeDelLevelIteratorWrapper() {}

  bool Valid() const override { return valid_; }

  bool HasUpperBound() const override {
    return (upper_bound_.empty() == false);
  }

  void SetUpperBound(const Slice& upper_bound) override {
    upper_bound_ = upper_bound;

    // TODO - Consider this - the level_iters_mngr_ should be updated as well,
    // but this is used in a very specific scenario - new CSK was found while
    // overlapping a range TS
    if (file_iter_wrapper_ != nullptr) {
      file_iter_wrapper_->SetUpperBound(upper_bound);
      UpdateValidity();
    }
  }

  const Slice& GetUpperBound() const override {
    assert(HasUpperBound());
    return upper_bound_;
  }

  void SeekToFirst() override {
    assert(file_iter_wrapper_ == nullptr);
    SkipToFirstAvailableRangeTs();
    UpdateValidity();
  }

  void SeekForward(const Slice& target) override {
    auto new_internal_iter =
        level_iters_mngr_.AdvanceRangeTsIterToFileContainingKey(target);

    if (new_internal_iter != nullptr) {
      if ((file_iter_wrapper_ == nullptr) ||
          (new_internal_iter != file_iter_wrapper_->Wrapped())) {
        // The target key is in a new file
        InitFileIterWrapper(new_internal_iter);
      }
      file_iter_wrapper_->SeekForward(target);
    }

    if ((file_iter_wrapper_ == nullptr) ||
        (file_iter_wrapper_->Valid() == false)) {
      CleanupFileIterWrapper();
      SkipToFirstAvailableRangeTs();
    }

    UpdateValidity();
  }

  void Next() override {
    assert(Valid());

    file_iter_wrapper_->Next();

    if (file_iter_wrapper_->Valid() == false) {
      CleanupFileIterWrapper();
      SkipToFirstAvailableRangeTs();
    }

    UpdateValidity();
  }

  RangeTombstone Tombstone() const override {
    assert(Valid());
    return file_iter_wrapper_->Tombstone();
  }

  Slice key() const override {
    assert(Valid());
    return file_iter_wrapper_->key();
  }

  std::string ToString() const override {
    if (Valid() == false) {
      return "Invalid";
    }
    return file_iter_wrapper_->ToString();
  }

 private:
  void InitFileIterWrapper(TruncatedRangeDelIterator* wrapped_iter) {
    auto new_wrapper = new TruncatedRangeDelIteratorWrapper(
        wrapped_iter, comparator_, upper_bound_, false /* is_iter_owner */);
    file_iter_wrapper_.reset(new_wrapper);
  }

  void CleanupFileIterWrapper() { file_iter_wrapper_.reset(); }

  void UpdateValidity() {
    valid_ = (file_iter_wrapper_ != nullptr) && (file_iter_wrapper_->Valid());
  }

  void SkipToFirstAvailableRangeTs() {
    while (level_iters_mngr_.HasNextRangeTsIter() &&
           ((file_iter_wrapper_ == nullptr) ||
            (file_iter_wrapper_->Valid() == false))) {
      auto next_internal_iter = level_iters_mngr_.NextRangeTsIter();
      if (next_internal_iter != nullptr) {
        InitFileIterWrapper(next_internal_iter);
        file_iter_wrapper_->SeekToFirst();
      }
    }
  }

 private:
  LevelFilesItersMngr& level_iters_mngr_;
  std::unique_ptr<TruncatedRangeDelIteratorWrapper> file_iter_wrapper_;
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
  const InternalKeyComparator* icomparator = nullptr;
  const Comparator* comparator = nullptr;
  std::shared_ptr<Logger> logger;

  std::unique_ptr<GlobalDelList::Iterator> del_list_iter;
};

struct LevelContext {
  std::unique_ptr<InternalIteratorWrapperBase> values_iter;
  TruncatedRangeDelIteratorWrapperBase* range_del_iter = nullptr;

  // std::string prev_del_key;

  // These are valid only when applicable
  ParsedInternalKey values_parsed_ikey;
  ValueCategory value_category = ValueCategory::NONE;

  bool new_csk_found_in_level = false;

  ~LevelContext() {
    delete range_del_iter;
    range_del_iter = nullptr;
  }
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

void ProcessCurrRangeTsVsDelList(GlobalContext& gc, LevelContext& lc, const RangeTombstone& range_ts) {
  assert(lc.range_del_iter->Valid());

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
            // Is this SeekForward()?
            lc.range_del_iter->SeekForward(del_elem.user_end_key);
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
  assert(lc.values_parsed_ikey.user_key.empty() == false);

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
        lc.values_iter->SeekForward(gc.del_list_iter->key().user_end_key);
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

namespace {
size_t loop_count = 0U;

Slice prev_value;
Slice prev_range_del;
RangeTombstone prev_range_ts;
DelElement prev_del_list {"", ""};

void ResetState() {
  prev_value.clear();
  prev_range_del.clear();
  prev_del_list.Clear();
}

void SetPrevRangeDel(LevelContext& lc) {
  prev_range_del = lc.range_del_iter->key();
  prev_range_ts = lc.range_del_iter->Tombstone();
  std::cout << "SetPrevRangeDel: " << lc.range_del_iter->ToString() << '\n';
}
void ClearPrevRangeDel() {
  prev_range_del.clear();
  prev_range_ts = RangeTombstone();
  std::cout << "ClearPrevRangeDel\n";
}

void InitState(GlobalContext& gc, LevelContext& lc) {
  ResetState();
  if (gs_validate_iters_progress == false) {
    return;
  }

  if (lc.values_iter->Valid()) {
    prev_value = lc.values_iter->key();
  } else {
    prev_value.clear();
  }

  if (lc.range_del_iter->Valid()) {
    SetPrevRangeDel(lc);
  } else {
    ClearPrevRangeDel();
  }

  if (gc.del_list_iter->Valid()) {
    prev_del_list = gc.del_list_iter->key();
  } else {
    prev_del_list.Clear();
  }
}

void ValidateItersProgress(GlobalContext& gc, LevelContext& lc) {
  if ((gs_validate_iters_progress == false) || (loop_count == 1U)) {
    return;
  }

  bool values_iter_progressed = false;
  bool range_del_iter_progressed = false;
  bool del_list_iter_progressed = false;

  auto was_values_iter_invalid = prev_value.empty();
  if (lc.values_iter->Valid()) {
    if (was_values_iter_invalid || (prev_value != lc.values_iter->key())) {
      values_iter_progressed = true;
      prev_value = lc.values_iter->key();
    }
  } else {
    if (was_values_iter_invalid == false) {
      values_iter_progressed = true;
    }
    prev_value.clear();
  }

  auto was_range_del_iter_invalid = prev_range_del.empty();
  if (lc.range_del_iter->Valid()) {
    if (was_range_del_iter_invalid || (AreRangeTssEqual(prev_range_ts, lc.range_del_iter->Tombstone()) == false)) {
      range_del_iter_progressed = true;
      SetPrevRangeDel(lc);
    }
  } else {
    if (was_range_del_iter_invalid == false) {
      range_del_iter_progressed = true;
    }
    ClearPrevRangeDel();
  }

  auto was_del_list_iter_invalid = prev_del_list.Empty();
  if (gc.del_list_iter->Valid()) {
    if (was_del_list_iter_invalid || (prev_del_list != gc.del_list_iter->key())) {
      del_list_iter_progressed = true;
      prev_del_list = gc.del_list_iter->key();
    }
  } else {
    if (was_del_list_iter_invalid == false) {
      del_list_iter_progressed = true;
    }
    prev_del_list.Clear();
  }

  if ((values_iter_progressed == false) && (range_del_iter_progressed == false) && (del_list_iter_progressed == false)) {
    std::cout << "\n>>>>>>>>>> NO PROGRESS: loop_count:" << loop_count << '\n';
    std::cout << "Prev Range-TS Slice:" << prev_range_del.ToString() << ", New Range-TS Slice:" << lc.range_del_iter->key().ToString() << '\n';
    std::cout << "Prev Range-TS:" << RangeTsToString(prev_range_ts) << ", New Range-TS:" << lc.range_del_iter->ToString() << '\n';
    assert(0);
  }
}
} // Unnamed Namespace

Status ProcessLogLevel(GlobalContext& gc, LevelContext& lc) {
  if (gc.target.empty()) {
    gc.del_list_iter->SeekToFirst();
    lc.values_iter->SeekToFirst();
    lc.range_del_iter->SeekToFirst();
  } else {
    gc.del_list_iter->Seek(gc.target);
    lc.values_iter->SeekForward(gc.target);
    lc.range_del_iter->SeekForward(gc.target);
  }

  loop_count = 0U;
  InitState(gc, lc);

  while ((lc.new_csk_found_in_level == false) &&
         (lc.values_iter->Valid() || lc.range_del_iter->Valid())) {

    ++loop_count;
    if (gs_report_iters_progress) std::cout << "loop_count:" << loop_count << '\n';
    // Validate at the top since continue will skip validation at the end of the loop
    ValidateItersProgress(gc, lc);

    if (lc.values_iter->Valid() == false) {
      // Range ts iter is Valid() values_iter is Invalid()
      ProcessCurrRangeTsVsDelList(gc, lc, lc.range_del_iter->Tombstone());
      continue;
    }

    // values_iter is valid
    [[maybe_unused]] auto parsing_status = ParseInternalKey(
        lc.values_iter->key(), &lc.values_parsed_ikey, true /* log_err_key */);
    assert(parsing_status.ok());
    lc.value_category = GetValueCategoryOfKey(lc.values_parsed_ikey.type);
    if (lc.value_category == ValueCategory::OTHER) {
      lc.values_iter->Next();
      continue;
    }

    if (lc.range_del_iter->Valid() == false) {
      ProcessCurrValuesIterVsDelList(gc, lc);
      continue;
    }

    auto range_ts = lc.range_del_iter->Tombstone();
    auto range_ts_vs_values_iter_key = CompareRangeTsToUserKey(
        range_ts, lc.values_parsed_ikey.user_key, gc.comparator,
        nullptr /* overlap_start_rel_pos */, nullptr /* overlap_end_rel_pos */);

    switch (range_ts_vs_values_iter_key) {
      case RelativePos::BEFORE:
        ProcessCurrRangeTsVsDelList(gc, lc, range_ts);
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
          auto was_new_csk_found = ProcessCurrValuesIterVsDelList(gc, lc);
          if (was_new_csk_found) {
            ProcessCurrRangeTsVsDelList(gc, lc, range_ts);
          }
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

  auto wrapped_values_iter = 
      super_version->mem->NewIterator(gc.mutable_read_options, &arena);
  lc.values_iter.reset(new InternalIteratorWrapper(
      wrapped_values_iter, gc.comparator, gc.csk, true /* is_iter_owner */));

  TruncatedRangeDelIterator* range_del_iter = nullptr;
  auto fragmented_range_del_iter = super_version->mem->NewRangeTombstoneIterator(
        gc.mutable_read_options, gc.seq_num, false /* immutable_memtable */);
  if (fragmented_range_del_iter == nullptr || fragmented_range_del_iter->empty()) {
    delete fragmented_range_del_iter;
    fragmented_range_del_iter = nullptr;
  } else {
    range_del_iter = new TruncatedRangeDelIterator(
            std::unique_ptr<FragmentedRangeTombstoneIterator>(fragmented_range_del_iter),
            gc.icomparator, nullptr /* smallest */, nullptr /* largest */);
  }
  lc.range_del_iter = new TruncatedRangeDelIteratorWrapper(
      range_del_iter, gc.comparator, gc.csk, true /* is_iter_owner */);

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
    lc.values_iter.reset(new InternalIteratorWrapper(memtbl_iters.memtbl_iter,
                                                     gc.comparator, gc.csk,
                                                     true /* is_iter_owner */));

    lc.range_del_iter = new TruncatedRangeDelIteratorWrapper(
        memtbl_iters.range_del_iter, gc.comparator, gc.csk,
        true /* is_iter_owner */);

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
  auto iters = super_version->current->GetIteratorsForLevel0(
      gc.mutable_read_options, file_options, false /* allow_unprepared_value */,
      &arena);

  if (gs_debug_prints)
    printf("Processing Level-0 Files. Num Files:%zu\n", iters.size());

  auto i = 1;
  for (auto& file_iters : iters) {
    LevelContext lc;
    lc.values_iter.reset(new InternalIteratorWrapper(file_iters.table_iter,
                                                     gc.comparator, gc.csk,
                                                     true /* is_iter_owner */));
    lc.range_del_iter = new TruncatedRangeDelIteratorWrapper(
        file_iters.range_ts_iter, gc.comparator, gc.csk,
        true /* is_iter_owner */);

    if (gs_debug_prints) printf("Processing Level-0 File #%d\n", i);
    auto status = ProcessLogLevel(gc, lc);
    if (status.ok() == false) {
      return status;
    }
    ++i;
  }
  return Status::OK();
}

Status ProcessLevelsGt0(SuperVersion* super_version, GlobalContext& gc,
                          const FileOptions& file_options, Arena& arena) {
  auto storage_info = super_version->current->storage_info();
  // assert(storage_info_->finalized_);

  for (int level = 1; level < storage_info->num_non_empty_levels(); level++) {
    if (storage_info->IsLevelEmpty(level)) {
      if (gs_debug_prints) {
        printf("Level-%d Is Empty\n", level);
      }
      continue;
    }

    LevelFilesItersMngr level_files_iter_mngr(
        super_version, gc.icomparator, gc.mutable_read_options, file_options,
        &arena, storage_info->LevelFilesBrief(level), gc.target, gc.csk);

    if (gs_debug_prints)
      printf("Processing Level-%d \n", level);

    LevelContext lc;
    lc.values_iter.reset(new InternalLevelIteratorWrapper(
        level_files_iter_mngr, gc.comparator, gc.csk));

    lc.range_del_iter = new TruncatedRangeDelLevelIteratorWrapper(
        level_files_iter_mngr, gc.comparator, gc.csk);

    if (gs_debug_prints) printf("Processing Level-%d\n", level);
    auto status = ProcessLogLevel(gc, lc);
    if (status.ok() == false) {
      if (gs_debug_prints) printf("Failed Processing Level-%d\n", level);
      return status;
    }
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

  StopWatch sw(immutable_db_options_.clock, immutable_db_options_.statistics.get(), DB_MULTIGET);

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
  gc.icomparator = &cfd->ioptions()->internal_comparator;
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

  if (status.ok()) {
    status = ProcessLevelsGt0(super_version, gc, file_options_, arena);
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
                            
  return GetSmallestAtOrAfter(read_options, column_family, "" /* target */, key, value);
}

}  // namespace ROCKSDB_NAMESPACE
