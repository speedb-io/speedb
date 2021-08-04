// Copyright (C) 2022 Speedb Ltd. All rights reserved.
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

#ifndef ROCKSDB_LITE

#include "plugin/speedb/memtable/hash_spd_rep.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>  // std::condition_variable
#include <list>
#include <vector>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/stl_wrappers.h"
#include "monitoring/histogram.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/options_type.h"
#include "util/hash.h"
#include "util/heap.h"
#include "util/murmurhash.h"

namespace ROCKSDB_NAMESPACE {
namespace {

enum class IterOption { kNone, kIter, kImmutable };

struct SortItem {
  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.

  SortItem* Next() { return next_.load(); }
  void SetNext(SortItem* item) { next_.store(item); }

  char* Key() { return key_; }

  bool IsIterPoint() { return iter_op_ != IterOption::kNone; }

  bool IsSorted() { return sorted_; }

  bool IsImmutable() { return iter_op_ == IterOption::kImmutable; }

  void SetSorted() { sorted_ = true; }

  void SetSortSetInfo(void* sort_set_info) { sort_set_info_ = sort_set_info; }

  void* GetSortSetInfo() { return sort_set_info_; }

  SortItem(IterOption iter_op = IterOption::kNone)
      : next_(nullptr), iter_op_(iter_op), sorted_(false) {}

 private:
  std::atomic<SortItem*> next_;
  IterOption iter_op_;
  bool sorted_;
  void* sort_set_info_;

  // Prohibit copying due to the below
  SortItem(const SortItem&) = delete;
  SortItem& operator=(const SortItem&) = delete;

 public:
  char key_[1];
};

struct BucketHeader {
  port::Mutex mutex_;  // this mutex probably wont cause delay
  std::list<SortItem*> items_;

  BucketHeader() { items_.clear(); }

  bool InternalContains(const MemTableRep::KeyComparator& comparator,
                        const char* check_key) {
    if (items_.size() == 0) {
      return false;
    }

    std::list<SortItem*>::iterator iter;

    for (iter = items_.begin(); iter != items_.end(); ++iter) {
      const char* key = (*iter)->key_;

      if (comparator(check_key, key) == 0) {
        return true;
      }
    }
    return false;
  }

  bool Contains(const MemTableRep::KeyComparator& comparator,
                const char* check_key) {
    MutexLock l(&mutex_);
    return InternalContains(comparator, check_key);
  }

  bool Add(SortItem* sort_item, const MemTableRep::KeyComparator& comparator,
           bool check_exist) {
    MutexLock l(&mutex_);
    if (check_exist) {
      if (InternalContains(comparator, sort_item->key_)) return false;
    }

    items_.push_front(sort_item);
    return true;
  }
};

enum class SeekOption {
  kInitForward,
  kInitBackward,
  kSwitchForward,
  kSwitchBackward,
};

class SortHeapItem {
 public:
  SortHeapItem() : add_vector_(nullptr), is_init_(false) {}

  ~SortHeapItem() {}

  // Move constructor.
  SortHeapItem(SortHeapItem&& other) noexcept
      : add_vector_(other.add_vector_),
        curr_iter_(other.curr_iter_),
        idx_(other.idx_),
        is_init_(other.is_init_) {}

  // Move assignment operator.
  SortHeapItem& operator=(SortHeapItem&& other) noexcept {
    if (this != &other) {
      // Copy the data pointer and its length from the
      // source object.
      add_vector_ = other.add_vector_;
      curr_iter_ = other.curr_iter_;
      idx_ = other.idx_;
      is_init_ = other.is_init_;
    }
    return *this;
  }

  char* Key() { return *curr_iter_; }

  char* Get() const { return *curr_iter_; }

  uint32_t GetIdx() { return idx_; }

  void Init(void* add_vector, std::vector<char*>::iterator curr_iter,
            uint32_t idx) {
    if (is_init_) return;
    add_vector_ = add_vector;
    curr_iter_ = curr_iter;
    idx_ = idx;
    is_init_ = true;
  }

 public:
  void* add_vector_;
  std::vector<char*>::iterator curr_iter_;
  uint32_t idx_;
  bool is_init_;
};

class IteratorComparator {
 public:
  IteratorComparator(const MemTableRep::KeyComparator& comparator,
                     bool up_direction)
      : comparator_(comparator), up_direction_(up_direction) {}

  bool operator()(SortHeapItem* a, SortHeapItem* b) const {
    return ((up_direction_) ? (comparator_(a->Get(), b->Get()) > 0)
                            : (comparator_(a->Get(), b->Get()) < 0));
  }

  void SetDirection(bool up_direction) { up_direction_ = up_direction; }

 private:
  const MemTableRep::KeyComparator& comparator_;
  bool up_direction_;
};

typedef BinaryHeap<SortHeapItem*, IteratorComparator> IterHeap;

class IterHeapInfo {
 public:
  IterHeapInfo(const MemTableRep::KeyComparator& comparator)
      : comparator_(comparator),
        iter_heap_(new IterHeap(IteratorComparator(comparator, true))) {}

  ~IterHeapInfo() { iter_heap_.get()->clear(); }

  void Init(uint32_t iter_sort_items_num) {
    sort_items_.reset(new SortHeapItem[iter_sort_items_num]);
  }

  void Reset(bool up_iter_direction) {
    iter_heap_.get()->clear();
    iter_heap_.reset(
        new IterHeap(IteratorComparator(comparator_, up_iter_direction)));
  }

  char* Key() const {
    char* requested_key = nullptr;
    if (iter_heap_.get()->size() != 0) {
      requested_key = iter_heap_.get()->top()->Key();
    }
    return requested_key;
  }

  bool Valid() const { return iter_heap_.get()->size() != 0; }

  SortHeapItem* Get() {
    if (!Valid()) return nullptr;
    uint32_t sort_item_idx = iter_heap_.get()->top()->GetIdx();
    return (&sort_items_.get()[sort_item_idx]);
  }

  SortHeapItem* Get(uint32_t idx) { return (&sort_items_.get()[idx]); }

  void Update(bool valid, SortHeapItem* sort_item) {
    if (valid) {
      iter_heap_.get()->replace_top(sort_item);
    } else {
      iter_heap_.get()->pop();
    }
  }

  void Insert(SortHeapItem* sort_item) { iter_heap_.get()->push(sort_item); }

 private:
  const MemTableRep::KeyComparator& comparator_;
  std::unique_ptr<SortHeapItem[]> sort_items_;
  std::unique_ptr<IterHeap> iter_heap_;
};

class SortVector {
 public:
  SortVector(uint32_t size_limit)
      : iter_point_(IterOption::kIter), size_limit_(size_limit) {
    items_.reserve(size_limit);
    smallest_key_ = nullptr;
    largest_key_ = nullptr;
  }

  ~SortVector() {}

  // Move constructor.
  SortVector(SortVector&& other) noexcept : iter_point_(IterOption::kIter) {
    items_.reserve(other.size_limit_);
  }

  bool IsEmpty();

  SortItem* GetIterPoint();

  bool Sort(const MemTableRep::KeyComparator& comparator);

  bool Insert(char* key);

  bool SeekForward(const MemTableRep::KeyComparator& comparator,
                   const char* seek_key, SortHeapItem* sort_item);
  bool SeekBackward(const MemTableRep::KeyComparator& comparator,
                    const char* seek_key, SortHeapItem* sort_item);

  bool SeekSwitchForward(const MemTableRep::KeyComparator& comparator,
                         const char* seek_key, SortHeapItem* sort_item);
  bool SeekSwitchBackward(const MemTableRep::KeyComparator& comparator,
                          const char* seek_key, SortHeapItem* sort_item);

  bool Seek(const MemTableRep::KeyComparator& comparator, const char* seek_key,
            SeekOption seek_op, SortHeapItem* sort_item, uint32_t idx);

  bool Next(SortHeapItem* sort_item);

  bool Prev(SortHeapItem* sort_item);

 private:
  SortItem iter_point_;
  std::vector<char*> items_;
  char* smallest_key_;
  char* largest_key_;
  uint32_t size_limit_;
  port::Mutex mutex_;
};

// SortVector implemntation

SortItem* SortVector::GetIterPoint() { return &iter_point_; }

bool SortVector::Insert(char* key) {
  items_.push_back(key);
  return (items_.size() == size_limit_) ? false : true;
}

bool SortVector::Sort(const MemTableRep::KeyComparator& comparator) {
  std::sort(items_.begin(), items_.end(), stl_wrappers::Compare(comparator));
  smallest_key_ = items_.front();
  largest_key_ = items_.back();

  return (items_.size() != 0) ? true : false;
}

bool SortVector::SeekForward(const MemTableRep::KeyComparator& comparator,
                             const char* seek_key, SortHeapItem* sort_item) {
  if (seek_key == nullptr) {
    sort_item->curr_iter_ = items_.begin();
  } else {
    if (comparator(largest_key_, seek_key) >= 0) {
      sort_item->curr_iter_ =
          std::lower_bound(items_.begin(), items_.end(), seek_key,
                           stl_wrappers::Compare(comparator));
    }
  }
  return (sort_item->curr_iter_ == items_.end()) ? false : true;
}

bool SortVector::SeekBackward(const MemTableRep::KeyComparator& comparator,
                              const char* seek_key, SortHeapItem* sort_item) {
  if (seek_key == nullptr) {
    sort_item->curr_iter_ = std::prev(items_.end());
  } else {
    if (comparator(smallest_key_, seek_key) <= 0) {
      sort_item->curr_iter_ =
          std::lower_bound(items_.begin(), items_.end(), seek_key,
                           stl_wrappers::Compare(comparator));
      if (comparator(*sort_item->curr_iter_, seek_key) > 0) {
        // need to backward the curr iter
        --sort_item->curr_iter_;
      }
    }
  }
  return (sort_item->curr_iter_ == items_.end()) ? false : true;
}

bool SortVector::SeekSwitchForward(const MemTableRep::KeyComparator& comparator,
                                   const char* seek_key,
                                   SortHeapItem* sort_item) {
  if (comparator(largest_key_, seek_key) <= 0) {
    // this addvector shouldnt be part of the iterator heap
    sort_item->curr_iter_ = items_.end();
  } else {
    if (sort_item->curr_iter_ != items_.end()) {
      ++sort_item->curr_iter_;
    } else {
      sort_item->curr_iter_ =
          std::upper_bound(items_.begin(), items_.end(), seek_key,
                           stl_wrappers::Compare(comparator));
    }
  }
  return (sort_item->curr_iter_ == items_.end()) ? false : true;
}

bool SortVector::SeekSwitchBackward(
    const MemTableRep::KeyComparator& comparator, const char* seek_key,
    SortHeapItem* sort_item) {
  if (comparator(smallest_key_, seek_key) >= 0) {
    // this addvector shouldnt be part of the iterator heap
    sort_item->curr_iter_ = items_.end();
  } else {
    if (sort_item->curr_iter_ != items_.end()) {
      --sort_item->curr_iter_;
    } else {
      sort_item->curr_iter_ =
          std::lower_bound(items_.begin(), items_.end(), seek_key,
                           stl_wrappers::Compare(comparator));
      sort_item->curr_iter_ = (sort_item->curr_iter_ == items_.begin())
                                  ? items_.end()
                                  : --sort_item->curr_iter_;
    }
  }
  return (sort_item->curr_iter_ == items_.end()) ? false : true;
}

bool SortVector::Seek(const MemTableRep::KeyComparator& comparator,
                      const char* seek_key, SeekOption seek_op,
                      SortHeapItem* sort_item, uint32_t idx) {
  if (items_.size() == 0) return false;
  sort_item->Init(this, this->items_.end(), idx);
  bool valid = false;
  switch (seek_op) {
    case SeekOption::kInitForward:
      valid = SeekForward(comparator, seek_key, sort_item);
      break;
    case SeekOption::kInitBackward:
      valid = SeekBackward(comparator, seek_key, sort_item);
      break;
    case SeekOption::kSwitchForward:
      valid = SeekSwitchForward(comparator, seek_key, sort_item);
      break;
    case SeekOption::kSwitchBackward:
      valid = SeekSwitchBackward(comparator, seek_key, sort_item);
      break;
  }
  return valid;
}

bool SortVector::Next(SortHeapItem* sort_item) {
  sort_item->curr_iter_++;
  return (sort_item->curr_iter_ != items_.end());
}

bool SortVector::Prev(SortHeapItem* sort_item) {
  if (sort_item->curr_iter_ == items_.begin()) {
    sort_item->curr_iter_ = items_.end();
  } else {
    sort_item->curr_iter_--;
  }
  return (sort_item->curr_iter_ != items_.end());
}

struct IterSortSettingInfo {
  std::list<std::shared_ptr<SortVector>>::iterator iter_anchor_;
  std::shared_ptr<SortVector> iter_sort_vector_;
  uint32_t iter_size_;
};

class SortVectorContainer {
 public:
  explicit SortVectorContainer(const MemTableRep::KeyComparator& comparator,
                               uint32_t switch_vector_limit)
      : items_count_(0),
        comparator_(comparator),
        switch_vector_limit_(switch_vector_limit),
        immutable_(false),
        anchor_item_(IterOption::kIter),
        immutable_item_(IterOption::kImmutable),
        sort_thread_terminate(false) {
    last_item_.store(&anchor_item_);
    last_sorted_item_.store(&anchor_item_);
    sort_thread_ = std::thread(&SortVectorContainer::SortThread, this);
  }

  ~SortVectorContainer() {
    {
      std::unique_lock<std::mutex> lck(sort_thread_mutex_);
      sort_thread_terminate.store(true);
      sort_thread_cv_.notify_one();
    }
    sort_thread_.join();
    empty_iter_sort_vectors_.clear();
  }

  void Insert(SortItem* new_item);

  void InitIterator(IterSortSettingInfo* sort_set_info);

  void SeekIter(std::list<std::shared_ptr<SortVector>>::iterator iter_anchor,
                IterHeapInfo* iter_heap_info, const char* seek_key,
                SeekOption seek_op);

  bool Next(SortHeapItem* sort_item);

  bool Prev(SortHeapItem* sort_item);

  void AdvanceAndSort(std::shared_ptr<SortVector> sort_vector);

  void Sort();
  void SortThread();
  void Immutable();

 public:
  // an atomic add item private the ability add without any lock
  std::atomic<SortItem*> last_item_;
  // an atomic item count allow us know when to create new sort vector
  std::atomic<uint32_t> items_count_;
  port::Mutex mutex_;

  std::list<std::shared_ptr<SortVector>> sort_vectors_;
  // this vector list is becuase we might did query on a quite memtable
  // BEFORE the memtable was immutable so no need to add a new sort vector to
  // heap. it needs to be immpmented better
  std::list<std::shared_ptr<SortVector>> empty_iter_sort_vectors_;
  const MemTableRep::KeyComparator& comparator_;

  uint32_t switch_vector_limit_;
  std::atomic<bool> immutable_;
  SortItem anchor_item_;

  port::RWMutex rwlock_;  // this is protect from being immutable and get iter
                          // in the same time
  SortItem immutable_item_;
  std::thread sort_thread_;
  std::mutex sort_thread_mutex_;
  std::atomic<bool> sort_thread_terminate;
  std::condition_variable sort_thread_cv_;
  std::mutex notify_sorted_mutex_;
  std::condition_variable notify_sorted_cv_;

  std::atomic<SortItem*> last_sorted_item_;
};

// SortVectorContainer implemanmtation

void SortVectorContainer::Insert(SortItem* new_item) {
  uint32_t items_count = items_count_.fetch_add(1);
  SortItem* prev_item = last_item_.exchange(new_item);
  prev_item->SetNext(new_item);

  if ((items_count % switch_vector_limit_) == 0) {
    // notify thread to create new
    std::unique_lock<std::mutex> lck(sort_thread_mutex_);
    sort_thread_cv_.notify_one();
  }
}

void SortVectorContainer::InitIterator(IterSortSettingInfo* sort_set_info) {
  SortItem* sort_item;
  bool immutable = false;

  {
    ReadLock rl(&rwlock_);  // see Immutable function
    immutable = immutable_.load();
    if (immutable) {
      sort_item = &immutable_item_;
    } else {
      sort_set_info->iter_sort_vector_ =
          std::make_shared<SortVector>(switch_vector_limit_);
      sort_item = sort_set_info->iter_sort_vector_->GetIterPoint();
      sort_item->SetSortSetInfo(static_cast<void*>(sort_set_info));
      SortItem* prev_item = last_item_.exchange(sort_item);
      prev_item->SetNext(sort_item);
      {
        std::unique_lock<std::mutex> lck(sort_thread_mutex_);
        sort_thread_cv_.notify_one();
      }
    }
  }

  {
    std::unique_lock<std::mutex> notify_lck(notify_sorted_mutex_);
    while (!sort_item->IsSorted()) notify_sorted_cv_.wait(notify_lck);
  }
  if (immutable) {
    // we are sorted and set! j
    sort_set_info->iter_anchor_ = sort_vectors_.begin();
    sort_set_info->iter_size_ = static_cast<uint32_t>(sort_vectors_.size());
  } else {
    // the info was set in the sort item
  }
  return;
}

void SortVectorContainer::SeekIter(
    std::list<std::shared_ptr<SortVector>>::iterator iter_anchor,
    IterHeapInfo* iter_heap_info, const char* seek_key, SeekOption seek_op) {
  std::list<std::shared_ptr<SortVector>>::iterator iter;
  uint32_t idx;
  for (iter = iter_anchor, idx = 0; iter != sort_vectors_.end();
       ++iter, ++idx) {
    SortHeapItem* sort_item = iter_heap_info->Get(idx);

    bool valid = (*iter)->Seek(comparator_, seek_key, seek_op, sort_item, idx);

    if (valid) iter_heap_info->Insert(sort_item);
  }
}

bool SortVectorContainer::Next(SortHeapItem* sort_item) {
  return (static_cast<SortVector*>(sort_item->add_vector_))->Next(sort_item);
}

bool SortVectorContainer::Prev(SortHeapItem* sort_item) {
  return (static_cast<SortVector*>(sort_item->add_vector_))->Prev(sort_item);
}

void SortVectorContainer::AdvanceAndSort(
    std::shared_ptr<SortVector> sort_vector) {
  sort_vectors_.front()->Sort(comparator_);
  std::shared_ptr<SortVector> push_sort_vector = sort_vector;
  if (!push_sort_vector)
    push_sort_vector = std::make_shared<SortVector>(switch_vector_limit_);
  sort_vectors_.push_front(push_sort_vector);
}

void SortVectorContainer::Sort() { sort_vectors_.front()->Sort(comparator_); }

void SortVectorContainer::SortThread() {
  bool should_exit = false;
  SortItem* last_loop_item = last_sorted_item_.load();
  // create first vector
  sort_vectors_.push_front(std::make_shared<SortVector>(switch_vector_limit_));

  while (!should_exit) {
    {
      std::unique_lock<std::mutex> lck(sort_thread_mutex_);
      while (!last_loop_item->Next() && !sort_thread_terminate.load())
        sort_thread_cv_.wait(lck);
    }
    // go over the items list  - create vector if needed and sort
    while (last_loop_item->Next()) {
      std::list<std::shared_ptr<SortVector>>::iterator last_sort_iter =
          sort_vectors_.begin();
      if (last_loop_item->Next()->IsIterPoint()) {  // an iter item
        if (last_loop_item->Next()->IsImmutable()) {
          // this is the last item! need to sort last vector and exit
          Sort();
          last_loop_item->Next()->SetSorted();
        } else {
          IterSortSettingInfo* sort_set_info =
              static_cast<IterSortSettingInfo*>(
                  last_loop_item->Next()->GetSortSetInfo());
          sort_set_info->iter_size_ =
              static_cast<uint32_t>(sort_vectors_.size());

          if (!last_loop_item->IsIterPoint()) {
            // sort the previous vector and create new one
            AdvanceAndSort(sort_set_info->iter_sort_vector_);
          } else {
            // need to add to the empty_iter_sort_vectors_
            // TBD AYELET
            empty_iter_sort_vectors_.push_back(
                std::make_shared<SortVector>(switch_vector_limit_));
          }
          sort_set_info->iter_anchor_ = last_sort_iter;
          last_loop_item->Next()->SetSorted();
        }
        last_sorted_item_.store(last_loop_item->Next());
        {
          // notify waiters iterators
          std::unique_lock<std::mutex> notify_lck(notify_sorted_mutex_);
          notify_sorted_cv_.notify_all();
        }
      } else {
        if (!(*last_sort_iter)->Insert(last_loop_item->Next()->Key())) {
          // we reach limit vector size sort and create new vector
          AdvanceAndSort(nullptr);
        }
      }
      last_loop_item = last_loop_item->Next();
    }
    if (sort_thread_terminate.load() || last_loop_item->IsImmutable()) {
      should_exit = true;  // thread should be terminated
    }
  }
}

void SortVectorContainer::Immutable() {
  {
    // make sure that no iter requests being performed
    WriteLock wl(&rwlock_);
    SortItem* prev_item = last_item_.exchange(&immutable_item_);
    prev_item->SetNext(&immutable_item_);
    immutable_.store(true);
  }
  {
    std::unique_lock<std::mutex> lck(sort_thread_mutex_);
    sort_thread_cv_.notify_one();
  }
}

class HashLocklessRep : public MemTableRep {
 public:
  HashLocklessRep(const MemTableRep::KeyComparator& compare,
                  Allocator* allocator, size_t bucket_size,
                  uint32_t add_vector_limit_size);

  KeyHandle Allocate(const size_t len, char** buf) override;

  void Insert(KeyHandle handle) override;

  bool InsertKey(KeyHandle handle) override;

  void InsertWithHintConcurrently(KeyHandle handle, void** hint) override;

  bool InsertKeyWithHintConcurrently(KeyHandle handle, void** hint) override;

  void InsertConcurrently(KeyHandle handle) override;

  bool InsertKeyConcurrently(KeyHandle handle) override;

  void MarkReadOnly() override;
  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~HashLocklessRep() override {}

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

 private:
  size_t bucket_size_;

  std::unique_ptr<BucketHeader[]> buckets_;

  const MemTableRep::KeyComparator& compare_;

  std::shared_ptr<SortVectorContainer> sort_vectors_cont_;

  size_t GetHash(const char* key) const {
    Slice slice = UserKey(key);
    return MurmurHash(slice.data(), static_cast<int>(slice.size()), 0) %
           bucket_size_;
  }
  bool InsertWithCheck(KeyHandle handle);

  BucketHeader* GetBucket(size_t i) const { return &buckets_.get()[i]; }

  BucketHeader* GetBucket(const char* key) const {
    return GetBucket(GetHash(key));
  }

  class Iterator : public MemTableRep::Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(std::shared_ptr<SortVectorContainer> sort_vectors_cont,
                      const MemTableRep::KeyComparator& comparator)
        : sort_vectors_cont_(sort_vectors_cont),
          iter_heap_info_(comparator),
          up_iter_direction_(true) {
      IterSortSettingInfo sort_set_info;
      sort_vectors_cont_->InitIterator(&sort_set_info);
      iter_anchor_ = sort_set_info.iter_anchor_;
      iter_sort_items_num_ = sort_set_info.iter_size_;
      // allocate iter_heap_info
      iter_heap_info_.Init(iter_sort_items_num_);
    }

    ~Iterator() override {}

    // Returns true if the iterator is positioned at a valid node.
    bool Valid() const override { return iter_heap_info_.Valid(); }

    // Returns the key at the current position.
    const char* key() const override { return iter_heap_info_.Key(); }

    void InternalSeek(const char* seek_key, SeekOption seek_op) {
      return sort_vectors_cont_->SeekIter(iter_anchor_, &iter_heap_info_,
                                          seek_key, seek_op);
    }

    void Reset(bool up_iter_direction) {
      up_iter_direction_ = up_iter_direction;
      iter_heap_info_.Reset(up_iter_direction_);
    }

    void ReverseDirection(bool up_iter_direction) {
      char* seek_key = iter_heap_info_.Key();
      Reset(up_iter_direction);
      InternalSeek(seek_key, (up_iter_direction) ? SeekOption::kSwitchForward
                                                 : SeekOption::kSwitchBackward);
    }

    void Advance() {
      SortHeapItem* sort_item = iter_heap_info_.Get();
      bool valid = (up_iter_direction_) ? sort_vectors_cont_->Next(sort_item)
                                        : sort_vectors_cont_->Prev(sort_item);
      iter_heap_info_.Update(valid, sort_item);
    }
    // Advances to the next position.
    void Next() override {
      if (!up_iter_direction_) {
        ReverseDirection(true);
      } else {
        Advance();
      }
    }

    // Advances to the previous position.
    void Prev() override {
      if (up_iter_direction_) {
        ReverseDirection(false);
      } else {
        Advance();
      }
    }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* memtable_key) override {
      Reset(true);
      InternalSeek(memtable_key ? memtable_key : EncodeKey(&tmp_, user_key),
                   SeekOption::kInitForward);
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      Reset(false);
      InternalSeek(memtable_key ? memtable_key : EncodeKey(&tmp_, user_key),
                   SeekOption::kInitBackward);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() if list is not empty.
    void SeekToFirst() override {
      Reset(true);
      InternalSeek(nullptr, SeekOption::kInitForward);
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() if list is not empty.
    void SeekToLast() override {
      Reset(false);
      InternalSeek(nullptr, SeekOption::kInitBackward);
    }

   private:
    std::shared_ptr<SortVectorContainer> sort_vectors_cont_;
    std::list<std::shared_ptr<SortVector>>::iterator iter_anchor_;
    uint32_t iter_sort_items_num_;
    IterHeapInfo iter_heap_info_;
    bool up_iter_direction_;

   protected:
    std::string tmp_;  // For passing to EncodeKey
  };

  class IteratorEmpty : public MemTableRep::Iterator {
   public:
    IteratorEmpty() {}

    ~IteratorEmpty() override {}

    // Returns true if the iterator is positioned at a valid node.
    bool Valid() const override { return false; }

    // Returns the key at the current position.
    const char* key() const override { return nullptr; }

    // Advances to the next position.
    void Next() override { return; }

    // Advances to the previous position.
    void Prev() override { return; }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& /* internal_key */,
              const char* /* memtable_key */) override {
      return;
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& /* internal_key */,
                     const char* /* memtable_key */) override {
      return;
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() if list is not empty.
    void SeekToFirst() override { return; }

    // Position at the last entry in list.
    // Final state of iterator is Valid() if list is not empty.
    void SeekToLast() override { return; }
  };
};

HashLocklessRep::HashLocklessRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator, size_t bucket_size,
                                 uint32_t add_list_limit_size)
    : MemTableRep(allocator), bucket_size_(bucket_size), compare_(compare) {
  sort_vectors_cont_ =
      std::make_shared<SortVectorContainer>(compare, add_list_limit_size);
  buckets_.reset(new BucketHeader[bucket_size]);
}

KeyHandle HashLocklessRep::Allocate(const size_t len, char** buf) {
  char* mem = allocator_->AllocateAligned(sizeof(SortItem) + len);
  SortItem* sort_item = new (mem) SortItem();
  *buf = sort_item->key_;
  return static_cast<void*>(sort_item);
}

void HashLocklessRep::Insert(KeyHandle handle) {
  SortItem* sort_item = static_cast<SortItem*>(handle);
  BucketHeader* bucket = GetBucket(sort_item->key_);
  bucket->Add(sort_item, this->compare_, false);
  // insert to later sorter list
  sort_vectors_cont_->Insert(sort_item);

  return;
}

bool HashLocklessRep::InsertWithCheck(KeyHandle handle) {
  SortItem* sort_item = static_cast<SortItem*>(handle);
  BucketHeader* bucket = GetBucket(sort_item->key_);

  if (!bucket->Add(sort_item, this->compare_, true)) {
    return false;
  }

  // insert to later sorter list
  sort_vectors_cont_->Insert(sort_item);

  return true;
}

bool HashLocklessRep::InsertKey(KeyHandle handle) {
  return InsertWithCheck(handle);
}

void HashLocklessRep::InsertWithHintConcurrently(KeyHandle handle, void**) {
  Insert(handle);
}

bool HashLocklessRep::InsertKeyWithHintConcurrently(KeyHandle handle, void**) {
  return InsertWithCheck(handle);
}

void HashLocklessRep::InsertConcurrently(KeyHandle handle) { Insert(handle); }

bool HashLocklessRep::InsertKeyConcurrently(KeyHandle handle) {
  return InsertWithCheck(handle);
}

bool HashLocklessRep::Contains(const char* key) const {
  BucketHeader* bucket = GetBucket(key);

  return bucket->Contains(this->compare_, key);
}

void HashLocklessRep::MarkReadOnly() { sort_vectors_cont_->Immutable(); }

size_t HashLocklessRep::ApproximateMemoryUsage() {
  // Memory is always allocated from the allocator.
  return 0;
}

void HashLocklessRep::Get(const LookupKey& k, void* callback_args,
                          bool (*callback_func)(void* arg, const char* entry)) {
  BucketHeader* bucket = GetBucket(k.memtable_key().data());
  MutexLock l(&bucket->mutex_);

  for (auto iter = bucket->items_.begin(); iter != bucket->items_.end();
       ++iter) {
    if (!callback_func(callback_args, (*iter)->key_)) {
      break;
    }
  }
}

MemTableRep::Iterator* HashLocklessRep::GetIterator(Arena* arena) {
  bool empty_iter = (sort_vectors_cont_->items_count_.load() == 0);
  if (!sort_vectors_cont_->immutable_.load()) empty_iter = true;
  if (arena != nullptr) {
    void* mem;
    if (empty_iter) {
      mem = arena->AllocateAligned(sizeof(IteratorEmpty));
      return new (mem) IteratorEmpty();
    } else {
      mem = arena->AllocateAligned(sizeof(Iterator));
      return new (mem) Iterator(sort_vectors_cont_, compare_);
    }
  }
  if (empty_iter) {
    return new IteratorEmpty();
  } else {
    return new Iterator(sort_vectors_cont_, compare_);
  }
}

static std::unordered_map<std::string, OptionTypeInfo> hash_spd_factory_info = {
#ifndef ROCKSDB_LITE
    {"bucket_count",
     {0, OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kDontSerialize /*Since it is part of the ID*/}},
#endif
};
}  // namespace

HashSpdRepFactory::HashSpdRepFactory(size_t bucket_count)
    : bucket_count_(bucket_count) {
  RegisterOptions("", &bucket_count_, &hash_spd_factory_info);
}

MemTableRep* HashSpdRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* /*transform*/, Logger* /*logger*/) {
  return new HashLocklessRep(compare, allocator, bucket_count_, 10000);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
