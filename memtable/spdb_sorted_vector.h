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

#include <atomic>

#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "util/heap.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace {
enum SeekOption { SEEK_FORWARD_OP, SEEK_BACKWARD_OP };

class SpdbVector {
 public:
  using Vec = std::vector<const char*>;
  using Iterator = Vec::iterator;

  SpdbVector(size_t switch_spdb_vector_limit)
      : SpdbVector(Vec(switch_spdb_vector_limit), 0) {}

  SpdbVector(Vec items, size_t n)
      : items_(std::move(items)),
        n_elements_(std::min(n, items_.size())),
        sorted_(n_elements_ > 0) {}

  void SetVectorListIter(
      std::list<std::shared_ptr<SpdbVector>>::iterator list_iter) {
    iter_ = list_iter;
  }

  std::list<std::shared_ptr<SpdbVector>>::iterator GetVectorListIter() {
    return iter_;
  }

  bool Add(const char* key);

  bool IsEmpty() const { return n_elements_ == 0; }

  bool Sort(const MemTableRep::KeyComparator& comparator);

  // find the first element that is >= the given key
  Iterator SeekForward(const MemTableRep::KeyComparator& comparator,
                       const Slice* seek_key);

  // find the first element that is <= given key
  Iterator SeekBackword(const MemTableRep::KeyComparator& comparator,
                        const Slice* seek_key);

  Iterator Seek(const MemTableRep::KeyComparator& comparator,
                const Slice* seek_key, bool up_iter_direction);

  bool Valid(const Iterator& iter) { return iter != items_.end(); }

  bool Next(Iterator& iter) {
    ++iter;
    return Valid(iter);
  }

  bool Prev(Iterator& iter) {
    if (iter == items_.begin()) {
      iter = items_.end();
      return false;
    }
    --iter;
    return true;
  }

  size_t Size() const { return n_elements_; }

  Iterator End() { return items_.end(); }

 private:
  Vec items_;
  std::atomic<size_t> n_elements_;
  std::atomic<bool> sorted_;
  // this is the iter the SpdbVector
  std::list<std::shared_ptr<SpdbVector>>::iterator iter_;
  port::RWMutexWr add_rwlock_;
};

using SpdbVectorPtr = std::shared_ptr<SpdbVector>;

class SortHeapItem {
 public:
  SortHeapItem() : spdb_vector_(0) {}
  SortHeapItem(SpdbVectorPtr spdb_vector, SpdbVector::Iterator curr_iter)
      : spdb_vector_(spdb_vector), curr_iter_(curr_iter) {}

  bool Valid() const { return spdb_vector_ && spdb_vector_->Valid(curr_iter_); }

  const char* Key() const { return *curr_iter_; }

  bool Next() { return spdb_vector_->Next(curr_iter_); }

  bool Prev() { return spdb_vector_->Prev(curr_iter_); }

 public:
  SpdbVectorPtr spdb_vector_;
  SpdbVector::Iterator curr_iter_;
};

class IteratorComparator {
 public:
  IteratorComparator(const MemTableRep::KeyComparator& comparator,
                     bool up_direction)
      : comparator_(comparator), up_direction_(up_direction) {}

  bool operator()(const SortHeapItem* a, const SortHeapItem* b) const {
    return ((up_direction_) ? (comparator_(a->Key(), b->Key()) > 0)
                            : (comparator_(a->Key(), b->Key()) < 0));
  }

  void SetDirection(bool up_direction) { up_direction_ = up_direction; }

 private:
  const MemTableRep::KeyComparator& comparator_;
  bool up_direction_;
};

using IterHeap = BinaryHeap<SortHeapItem*, IteratorComparator>;

class IterHeapInfo {
 public:
  IterHeapInfo(const MemTableRep::KeyComparator& comparator)
      : iter_heap_(new IterHeap(IteratorComparator(comparator, true))),
        comparator_(comparator) {}

  void Reset(bool up_iter_direction) {
    iter_heap_.reset(
        new IterHeap(IteratorComparator(comparator_, up_iter_direction)));
  }

  const char* Key() const {
    if (iter_heap_.get()->size() != 0) {
      return iter_heap_.get()->top()->Key();
    }
    return nullptr;
  }

  bool Valid() const { return iter_heap_.get()->size() != 0; }

  SortHeapItem* Get() {
    if (!Valid()) {
      return nullptr;
    }
    return iter_heap_.get()->top();
  }

  void Update(SortHeapItem* sort_item) {
    if (sort_item->Valid()) {
      iter_heap_.get()->replace_top(sort_item);
    } else {
      iter_heap_.get()->pop();
    }
  }

  void Insert(SortHeapItem* sort_item) { iter_heap_.get()->push(sort_item); }

  bool Prev(SortHeapItem* sort_item);

  const MemTableRep::KeyComparator& Comparator() const { return comparator_; }

 private:
  std::unique_ptr<IterHeap> iter_heap_;
  const MemTableRep::KeyComparator& comparator_;
};

using IterAnchors = std::list<SortHeapItem*>;

class SpdbVectorContainer {
 public:
  SpdbVectorContainer(const MemTableRep::KeyComparator& comparator)
      : comparator_(comparator),
        switch_spdb_vector_limit_(10000),
        immutable_(false),
        num_elements_(0) {
    SpdbVectorPtr spdb_vector(new SpdbVector(switch_spdb_vector_limit_));
    spdb_vectors_.push_front(spdb_vector);
    spdb_vector->SetVectorListIter(std::prev(spdb_vectors_.end()));
    curr_vector_.store(spdb_vector.get());
    sort_thread_ = std::thread(&SpdbVectorContainer::SortThread, this);
  }

  ~SpdbVectorContainer() {
    MarkReadOnly();
    sort_thread_.join();
  }

  bool InternalInsert(const char* key);

  void Insert(const char* key);

  bool IsEmpty() const;

  bool IsReadOnly() const { return immutable_.load(); }

  // create a list of current vectors
  bool InitIterator(IterAnchors& iter_anchor);

  void InitIterator(IterAnchors& iter_anchor,
                    std::list<SpdbVectorPtr>::iterator start,
                    std::list<SpdbVectorPtr>::iterator last);

  // seek & build the heap
  void SeekIter(const IterAnchors& iter_anchor, IterHeapInfo* iter_heap_info,
                const Slice* seek_key, bool up_iter_direction);

  void MarkReadOnly() {
    {
      std::unique_lock<std::mutex> lck(sort_thread_mutex_);
      WriteLock wl(&spdb_vectors_add_rwlock_);
      immutable_.store(true);
    }
    sort_thread_cv_.notify_one();
  }
  const MemTableRep::KeyComparator& GetComparator() const {
    return comparator_;
  }

 private:
  void SortThread();

 private:
  port::RWMutexWr spdb_vectors_add_rwlock_;
  port::Mutex spdb_vectors_mutex_;
  std::list<SpdbVectorPtr> spdb_vectors_;
  std::atomic<SpdbVector*> curr_vector_;
  const MemTableRep::KeyComparator& comparator_;
  const size_t switch_spdb_vector_limit_;
  std::atomic<bool> immutable_;
  // sort thread info
  std::atomic<size_t> num_elements_;
  std::thread sort_thread_;
  std::mutex sort_thread_mutex_;
  std::condition_variable sort_thread_cv_;
};

class SpdbVectorIterator : public MemTableRep::Iterator {
 public:
  // Initialize an iterator over the specified list.
  // The returned iterator is not valid.
  SpdbVectorIterator(std::shared_ptr<SpdbVectorContainer> spdb_vectors_cont,
                     const MemTableRep::KeyComparator& comparator)
      : spdb_vectors_cont_holder_(spdb_vectors_cont),
        spdb_vectors_cont_(spdb_vectors_cont.get()),
        iter_heap_info_(comparator),
        up_iter_direction_(true) {
    spdb_vectors_cont_->InitIterator(iter_anchor_);
  }

  SpdbVectorIterator(SpdbVectorContainer* spdb_vectors_cont,
                     const MemTableRep::KeyComparator& comparator,
                     std::list<SpdbVectorPtr>::iterator start,
                     std::list<SpdbVectorPtr>::iterator last)
      : spdb_vectors_cont_(spdb_vectors_cont),
        iter_heap_info_(comparator),
        up_iter_direction_(true) {
    // this is being called only from Merge , meaning we must have a non empty
    // vectors!!!
    spdb_vectors_cont_->InitIterator(iter_anchor_, start, last);
  }

  ~SpdbVectorIterator() override {
    for (SortHeapItem* item : iter_anchor_) {
      delete item;
    }
  }

  // Returns true if the iterator is positioned at a valid node.
  bool Valid() const override { return iter_heap_info_.Valid(); }

  // Returns the key at the current position.
  const char* key() const override { return iter_heap_info_.Key(); }

  void InternalSeek(const Slice* seek_key) {
    return spdb_vectors_cont_->SeekIter(iter_anchor_, &iter_heap_info_,
                                        seek_key, up_iter_direction_);
  }

  void Reset(bool up_iter_direction) {
    up_iter_direction_ = up_iter_direction;
    iter_heap_info_.Reset(up_iter_direction_);
  }

  void ReverseDirection(bool up_iter_direction) {
    const Slice seek_key =
        iter_heap_info_.Comparator().decode_key(iter_heap_info_.Key());
    Reset(up_iter_direction);
    InternalSeek(&seek_key);
  }

  void Advance() {
    SortHeapItem* sort_item = iter_heap_info_.Get();
    if (up_iter_direction_) {
      sort_item->Next();
    } else {
      sort_item->Prev();
    }
    iter_heap_info_.Update(sort_item);
  }

  // Advances to the next position.
  void Next() override {
    if (!up_iter_direction_) {
      ReverseDirection(true);
    }
    Advance();
  }

  // Advances to the previous position.
  void Prev() override {
    if (up_iter_direction_) {
      ReverseDirection(false);
    }
    Advance();
  }

  // Advance to the first entry with a key >= target
  void Seek(const Slice& internal_key,
            const char* /* memtable_key */) override {
    Reset(true);
    InternalSeek(&internal_key);
  }

  // Retreat to the last entry with a key <= target
  void SeekForPrev(const Slice& internal_key,
                   const char* /* memtable_key */) override {
    Reset(false);
    InternalSeek(&internal_key);
  }

  // Position at the first entry in list.
  // Final state of iterator is Valid() if list is not empty.
  void SeekToFirst() override {
    Reset(true);
    InternalSeek(nullptr);
  }

  // Position at the last entry in list.
  // Final state of iterator is Valid() if list is not empty.
  void SeekToLast() override {
    Reset(false);
    InternalSeek(nullptr);
  }

 private:
  std::shared_ptr<SpdbVectorContainer> spdb_vectors_cont_holder_;
  SpdbVectorContainer* spdb_vectors_cont_;
  IterAnchors iter_anchor_;
  IterHeapInfo iter_heap_info_;
  bool up_iter_direction_;
};
class SpdbVectorIteratorEmpty : public MemTableRep::Iterator {
 public:
  SpdbVectorIteratorEmpty() {}

  ~SpdbVectorIteratorEmpty() override {}

  // Returns true if the iterator is positioned at a valid node.
  bool Valid() const override { return false; }

  bool IsEmpty() override { return true; }

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

 private:
};
}  // namespace

}  // namespace ROCKSDB_NAMESPACE
