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
#include <list>
#include <vector>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/stl_wrappers.h"
#include "monitoring/histogram.h"
#include "plugin/speedb/memtable/spdb_sort_list.h"
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

struct SpdbKeyHandle {
  SpdbKeyHandle* GetNextBucketItem() {
    return bucket_item_link_.load(std::memory_order_acquire);
  }
  void SetNextBucketItem(SpdbKeyHandle* handle) {
    bucket_item_link_.store(handle, std::memory_order_release);
  }
  SpdbKeyHandle* GetNextSortedItem() {
    return spdb_item_link_.load(std::memory_order_acquire);
  }
  void SetNextSortedItem(SpdbKeyHandle* handle) {
    spdb_item_link_.store(handle, std::memory_order_release);
  }
  void SetKey(char* key) { spdb_sorted_key_ = key; }
  char* Key() { return spdb_sorted_key_; }
  void Init() {
    bucket_item_link_ = nullptr;
    spdb_item_link_ = nullptr;
    spdb_sorted_key_ = nullptr;
  }

  SpdbKeyHandle()
      : bucket_item_link_(nullptr),
        spdb_item_link_(nullptr),
        spdb_sorted_key_(nullptr) {}

 public:
  // this is for the bucket item list (should be very small)
  std::atomic<SpdbKeyHandle*> bucket_item_link_;
  // this is for the spdb item list its a next link
  std::atomic<SpdbKeyHandle*> spdb_item_link_;
  // Prohibit copying due to the below
  SpdbKeyHandle(const SpdbKeyHandle&) = delete;
  SpdbKeyHandle& operator=(const SpdbKeyHandle&) = delete;

 public:
  char* spdb_sorted_key_;
};

struct BucketHeader {
  port::Mutex mutex_;  // this mutex probably wont cause delay
  std::atomic<uint32_t> size_ = 0;
  std::atomic<SpdbKeyHandle*> items_ = nullptr;

  BucketHeader() {}

  bool Contains(const char* check_key,
                const MemTableRep::KeyComparator& comparator) {
    SpdbKeyHandle* anchor = items_.load(std::memory_order_acquire);
    for (auto k = anchor; k != nullptr; k = k->GetNextBucketItem()) {
      const int cmp_res = comparator(k->spdb_sorted_key_, check_key);
      if (cmp_res == 0) {
        return true;
      }
      if (cmp_res > 0) {
        break;
      }
    }
    return false;
  }

  bool Add(SpdbKeyHandle* handle,
           const MemTableRep::KeyComparator& comparator) {
    MutexLock l(&mutex_);
    SpdbKeyHandle* iter = items_.load(std::memory_order_acquire);
    SpdbKeyHandle* prev = nullptr;

    for (size_t i = 0; i < size_; i++) {
      const int cmp_res =
          comparator(iter->spdb_sorted_key_, handle->spdb_sorted_key_);
      if (cmp_res == 0) {
        // exist!
        return false;
      }

      if (cmp_res > 0) {
        // need to insert before
        break;
      }
      prev = iter;
      iter = iter->GetNextBucketItem();
    }

    handle->SetNextBucketItem(iter);
    if (prev) {
      prev->SetNextBucketItem(handle);
    } else {
      items_ = handle;
    }

    size_++;

    return true;
  }

  SpdbKeyHandle* InternalSeek(const char* seek_entry,
                              const MemTableRep::KeyComparator& comparator) {
    uint32_t bucket_size = size_.load(std::memory_order_acquire);
    if (bucket_size == 0) {
      return nullptr;
    }
    auto iter = items_.load(std::memory_order_acquire);
    for (uint32_t i = 0; i < bucket_size;
         iter = iter->GetNextBucketItem(), i++) {
      const int cmp_res = comparator(iter->spdb_sorted_key_, seek_entry);
      if (cmp_res == 0) {
        return iter;
      }
    }
    return nullptr;
  }

  void Get(const LookupKey& k, const MemTableRep::KeyComparator& comparator,
           void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) const {
    uint32_t bucket_size = size_.load(std::memory_order_acquire);
    if (bucket_size == 0) {
      return;
    }
    auto iter = items_.load(std::memory_order_acquire);
    uint32_t i = 0;
    for (; i < bucket_size; iter = iter->GetNextBucketItem(), i++) {
      if (comparator(iter->spdb_sorted_key_, k.internal_key()) >= 0) {
        break;
      }
    }

    for (; i < bucket_size; iter = iter->GetNextBucketItem(), i++) {
      if (!callback_func(callback_args, iter->spdb_sorted_key_)) {
        break;
      }
    }
  }
};

struct SpdbHashTable {
  std::vector<BucketHeader> buckets_;

  SpdbHashTable(size_t n_buckets) : buckets_(n_buckets) {}

  bool Add(SpdbKeyHandle* val, const MemTableRep::KeyComparator& comparator) {
    BucketHeader* bucket = GetBucket(val->Key(), comparator);
    return bucket->Add(val, comparator);
  }

  bool Contains(const char* check_key,
                const MemTableRep::KeyComparator& comparator) const {
    BucketHeader* bucket = GetBucket(check_key, comparator);
    return bucket->Contains(check_key, comparator);
  }

  SpdbKeyHandle* InternalSeek(const char* seek_entry,
                              const MemTableRep::KeyComparator& comparator) {
    BucketHeader* bucket = GetBucket(seek_entry, comparator);
    return bucket->InternalSeek(seek_entry, comparator);
  }

  void Get(const LookupKey& k, const MemTableRep::KeyComparator& comparator,
           void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) const {
    BucketHeader* bucket = GetBucket(k.internal_key(), comparator);
    bucket->Get(k, comparator, callback_args, callback_func);
  }

 private:
  static size_t GetHash(const Slice& user_key_without_ts) {
    return MurmurHash(user_key_without_ts.data(),
                      static_cast<int>(user_key_without_ts.size()), 0);
  }

  static Slice UserKeyWithoutTimestamp(
      const Slice internal_key, const MemTableRep::KeyComparator& compare) {
    auto key_comparator = static_cast<const MemTable::KeyComparator*>(&compare);
    const Comparator* user_comparator =
        key_comparator->comparator.user_comparator();
    const size_t ts_sz = user_comparator->timestamp_size();
    return ExtractUserKeyAndStripTimestamp(internal_key, ts_sz);
  }

  BucketHeader* GetBucket(const char* key,
                          const MemTableRep::KeyComparator& comparator) const {
    return GetBucket(comparator.decode_key(key), comparator);
  }

  BucketHeader* GetBucket(const Slice& internal_key,
                          const MemTableRep::KeyComparator& comparator) const {
    const size_t hash =
        GetHash(UserKeyWithoutTimestamp(internal_key, comparator));
    BucketHeader* bucket =
        const_cast<BucketHeader*>(&buckets_[hash % buckets_.size()]);
    return bucket;
  }
};

struct SpdbSort {
  SpdbSort(const MemTableRep::KeyComparator& compare, Allocator* allocator)
      : compare_(compare), spdb_sorted_list_(compare, allocator) {}
  const MemTableRep::KeyComparator& compare_;
  SpdbSortedList<const MemTableRep::KeyComparator&> spdb_sorted_list_;
};

class HashSpdRep : public MemTableRep {
 public:
  HashSpdRep(const MemTableRep::KeyComparator& compare, Allocator* allocator,
             size_t bucket_size);
  void SpdbSortThread();

  HashSpdRep(Allocator* allocator, size_t bucket_size);
  void PostCreate(const MemTableRep::KeyComparator& compare,
                  Allocator* allocator);

  KeyHandle Allocate(const size_t len, char** buf) override;

  bool InsertInternal(KeyHandle handle, bool concurrently);

  void Insert(KeyHandle handle) override {
    InsertInternal(handle, false);
    return;
  }

  bool InsertKey(KeyHandle handle) override {
    return InsertInternal(handle, false);
  }

  bool InsertKeyWithHint(KeyHandle handle, void**) override {
    return InsertInternal(handle, false);
  }

  bool InsertKeyWithHintConcurrently(KeyHandle handle, void**) override {
    return InsertInternal(handle, true);
  }

  bool InsertKeyConcurrently(KeyHandle handle) override {
    return InsertInternal(handle, true);
  }

  void InsertWithHintConcurrently(KeyHandle handle, void**) override {
    InsertInternal(handle, true);
    return;
  }

  void InsertConcurrently(KeyHandle handle) override {
    InsertInternal(handle, true);
    return;
  }

  void MarkReadOnly() override;

  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  SpdbKeyHandle* InternalSeek(const char* seek_entry);

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~HashSpdRep() override;

  // Iteration over the contents of a spdb sorted list
  class SpdbIterator : public MemTableRep::Iterator {
    SpdbSortedList<const MemTableRep::KeyComparator&>::Iterator iter_;
    HashSpdRep* rep_;

   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit SpdbIterator(
        const SpdbSortedList<const MemTableRep::KeyComparator&>* list,
        HashSpdRep* rep)
        : iter_(list), rep_(rep) {}

    ~SpdbIterator() override {}

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override { return iter_.key(); }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override { iter_.Next(); }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override { iter_.Prev(); }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* memtable_key) override {
      const char* seek_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
      // first try to see if we get it fast
      SpdbKeyHandle* seek_handle = rep_->InternalSeek(seek_key);
      if (seek_handle) {
        iter_.SetSeek(seek_handle->Key());
      } else {
        iter_.Seek(seek_key);
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      const char* seek_key =
          (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
      iter_.SeekForPrev(seek_key);
    }

    void RandomSeek() override { iter_.RandomSeek(); }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() override { iter_.SeekToLast(); }

   protected:
    std::string tmp_;  // For passing to EncodeKey
  };

  class SpdbIteratorEmpty : public MemTableRep::Iterator {
   public:
    SpdbIteratorEmpty() {}

    ~SpdbIteratorEmpty() override {}

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

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;
  bool IsImmutable() { return immutable_.load(); }

  bool IsEmpty() { return elements_num_ == 0; }

 private:
  SpdbHashTable spdb_hash_table_;
  std::shared_ptr<SpdbSort> spdb_sort_;
  std::atomic<SpdbKeyHandle*> last_item_;
  SpdbKeyHandle anchor_item_;
  std::atomic<bool> immutable_ = false;
  std::atomic<uint64_t> elements_num_ = 0;
  std::atomic<uint64_t> sort_barrier_ = 0;

  // sort thread info
  std::atomic<bool> sort_thread_terminate_ = false;
  std::atomic<bool> sort_thread_init_ = false;
  std::thread sort_thread_;
  std::mutex sort_thread_mutex_;
  std::condition_variable sort_thread_cv_;
  std::mutex notify_sorted_mutex_;
  std::condition_variable notify_sorted_cv_;
};

HashSpdRep::HashSpdRep(const MemTableRep::KeyComparator& compare,
                       Allocator* allocator, size_t bucket_size)
    : HashSpdRep(allocator, bucket_size) {
  PostCreate(compare, allocator);
}

HashSpdRep::~HashSpdRep() {
  if (sort_thread_init_.load()) {
    {
      std::unique_lock<std::mutex> lck(sort_thread_mutex_);
      sort_thread_terminate_.store(true);
    }
    sort_thread_cv_.notify_one();
    // make sure the thread got the termination notify
    sort_thread_.join();
  }
}

void HashSpdRep::SpdbSortThread() {
  bool should_exit = false;
  SpdbKeyHandle* last_loop_item = last_item_.load();
  {
    std::unique_lock<std::mutex> lck(sort_thread_mutex_);
    sort_thread_init_.store(true);
  }
  sort_thread_cv_.notify_one();

  while (!should_exit) {
    {
      std::unique_lock<std::mutex> lck(sort_thread_mutex_);
      while (!sort_thread_terminate_.load()) sort_thread_cv_.wait(lck);
    }
    if (sort_thread_terminate_.load()) {
      should_exit = true;
      break;
    }

    while (sort_barrier_ < elements_num_.load()) {
      if (!last_loop_item->GetNextSortedItem()) {
        continue;
      }
      last_loop_item = last_loop_item->GetNextSortedItem();
      spdb_sort_->spdb_sorted_list_.Insert(last_loop_item->Key(), false);
      {
        std::unique_lock<std::mutex> notify_lck(notify_sorted_mutex_);
        sort_barrier_.fetch_add(1);
        notify_sorted_cv_.notify_all();
      }
      if (sort_thread_terminate_.load()) {
        should_exit = true;
        break;
      }
    }
  }
}

HashSpdRep::HashSpdRep(Allocator* allocator, size_t bucket_size)
    : MemTableRep(allocator), spdb_hash_table_(bucket_size), anchor_item_() {}

void HashSpdRep::PostCreate(const MemTableRep::KeyComparator& compare,
                            Allocator* allocator) {
  allocator_ = allocator;
  spdb_sort_ = std::make_shared<SpdbSort>(compare, allocator);

  last_item_.store(&anchor_item_);
  sort_thread_ = std::thread(&HashSpdRep::SpdbSortThread, this);
  // need to verify the thread was executed
  {
    std::unique_lock<std::mutex> lck(sort_thread_mutex_);
    while (!sort_thread_init_.load()) {
      sort_thread_cv_.wait(lck);
    }
  }
}

KeyHandle HashSpdRep::Allocate(const size_t len, char** buf) {
  char* spdb_sorted_key;
  SpdbKeyHandle* handle = reinterpret_cast<SpdbKeyHandle*>(
      spdb_sort_->spdb_sorted_list_.AllocateSpdbItem(len, sizeof(SpdbKeyHandle),
                                                     &spdb_sorted_key));
  handle->Init();
  handle->SetKey(spdb_sorted_key);
  *buf = spdb_sorted_key;
  return handle;
}

bool HashSpdRep::InsertInternal(KeyHandle handle, bool concurrently) {
  SpdbKeyHandle* spdb_handle = static_cast<SpdbKeyHandle*>(handle);
  if (!spdb_hash_table_.Add(spdb_handle, spdb_sort_->compare_)) {
    return false;
  }
  spdb_sort_->spdb_sorted_list_.Insert(spdb_handle->Key(), concurrently);
  elements_num_.fetch_add(1);
  return true;
}

bool HashSpdRep::Contains(const char* key) const {
  return spdb_hash_table_.Contains(key, spdb_sort_->compare_);
}

void HashSpdRep::MarkReadOnly() { immutable_.store(true); }

size_t HashSpdRep::ApproximateMemoryUsage() {
  // Memory is always allocated from the allocator.
  return 0;
}

SpdbKeyHandle* HashSpdRep::InternalSeek(const char* seek_entry) {
  return spdb_hash_table_.InternalSeek(seek_entry, spdb_sort_->compare_);
}

void HashSpdRep::Get(const LookupKey& k, void* callback_args,
                     bool (*callback_func)(void* arg, const char* entry)) {
  spdb_hash_table_.Get(k, spdb_sort_->compare_, callback_args, callback_func);
}

MemTableRep::Iterator* HashSpdRep::GetIterator(Arena* arena) {
  const bool empty_iter = IsEmpty();

  if (arena != nullptr) {
    void* mem;
    if (empty_iter) {
      mem = arena->AllocateAligned(sizeof(SpdbIteratorEmpty));
      return new (mem) SpdbIteratorEmpty();
    } else {
      mem = arena->AllocateAligned(sizeof(SpdbIterator));
      return new (mem) SpdbIterator(&spdb_sort_->spdb_sorted_list_, this);
    }
  } else {
    if (empty_iter) {
      return new SpdbIteratorEmpty();
    } else {
      return new SpdbIterator(&spdb_sort_->spdb_sorted_list_, this);
    }
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
  Init();
}

MemTableRep* HashSpdRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* /*transform*/, Logger* /*logger*/) {
  return new HashSpdRep(compare, allocator, bucket_count_);
}

MemTableRep* HashSpdRepFactory::PreCreateMemTableRep() {
  MemTableRep* hash_spd = new HashSpdRep(nullptr, bucket_count_);
  return hash_spd;
}

void HashSpdRepFactory::PostCreateMemTableRep(
    MemTableRep* switch_mem, const MemTableRep::KeyComparator& compare,
    Allocator* allocator, const SliceTransform* /*transform*/,
    Logger* /*logger*/) {
  static_cast<HashSpdRep*>(switch_mem)->PostCreate(compare, allocator);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
