//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <limits>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/db.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

class SnapshotList;

struct SnapshotRecord {
  SnapshotRecord(SequenceNumber number_, int64_t unix_time_,
                 bool is_write_conflict_boundary_)
      : number(number_),
        unix_time(unix_time_),
        is_write_conflict_boundary(is_write_conflict_boundary_) {}

  const SequenceNumber number;

  const int64_t unix_time;

 private:
  friend class SnapshotList;

  void ref(uint32_t refs = 1) {
    refcount_.fetch_add(refs, std::memory_order_relaxed);
  }

  size_t unref(uint32_t refs = 1) const {
    return refcount_.fetch_sub(refs, std::memory_order_relaxed) - refs;
  }

  // Will this snapshot be used by a Transaction to do write-conflict checking?
  bool is_write_conflict_boundary;

  // SnapshotRecord is kept in a doubly-linked circular list
  SnapshotRecord* prev_ = nullptr;
  SnapshotRecord* next_ = nullptr;

  mutable std::atomic<size_t> refcount_{0};
};

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
class SnapshotImpl : public Snapshot {
 public:
  explicit SnapshotImpl(bool is_write_conflict_boundary)
      : Snapshot(),
        number_(0),
        list_(nullptr),
        record_(nullptr),
        is_write_conflict_boundary_(is_write_conflict_boundary) {}
  explicit SnapshotImpl(SequenceNumber number) : SnapshotImpl(false) {
    number_ = number;
  }

  ~SnapshotImpl() {
    assert(record_ == nullptr);
    assert(list_ == nullptr);
  }

  // It indicates the smallest uncommitted data at the time the snapshot was
  // taken. This is currently used by WritePrepared transactions to limit the
  // scope of queries to IsInSnapshot.
  SequenceNumber min_uncommitted_ = kMinUnCommittedSeq;

  virtual SequenceNumber GetSequenceNumber() const override { return number_; }

 private:
  friend class SnapshotList;

  SequenceNumber number_;

  SnapshotList* list_;                 // just for sanity checks
  std::atomic<SnapshotRecord*> record_;

  // Will this snapshot be used by a Transaction to do write-conflict checking?
  const bool is_write_conflict_boundary_;
};

class SnapshotList {
 public:
  SnapshotList() : list_(0xFFFFFFFFL, 0, false) {
    list_.prev_ = &list_;
    list_.next_ = &list_;
    count_ = 0;
  }

  // No copy-construct.
  SnapshotList(const SnapshotList&) = delete;

  bool empty() const { return list_.next_ == &list_; }
  SnapshotRecord* oldest() const {
    assert(!empty());
    return list_.next_;
  }
  SnapshotRecord* newest() const {
    assert(!empty());
    return list_.prev_;
  }

  bool RefCached(SnapshotImpl* s, SequenceNumber seq);

  // REQUIRES: DB mutex held
  bool New(SnapshotImpl* s, SequenceNumber seq, int64_t unix_time);

  bool Unref(const SnapshotImpl* s);

  // REQUIRES: DB mutex held
  bool Delete(const SnapshotImpl* s);

  // REQUIRES: DB mutex held
  bool ResetCached();

  // retrieve all snapshot numbers up until max_seq. They are sorted in
  // ascending order (with no duplicates).
  std::vector<SequenceNumber> GetAll(
      SequenceNumber* oldest_write_conflict_snapshot = nullptr,
      const SequenceNumber& max_seq = kMaxSequenceNumber) const {
    std::vector<SequenceNumber> ret;
    GetAll(&ret, oldest_write_conflict_snapshot, max_seq);
    return ret;
  }

  void GetAll(std::vector<SequenceNumber>* snap_vector,
              SequenceNumber* oldest_write_conflict_snapshot = nullptr,
              const SequenceNumber& max_seq = kMaxSequenceNumber) const {
    std::vector<SequenceNumber>& ret = *snap_vector;
    // So far we have no use case that would pass a non-empty vector
    assert(ret.size() == 0);

    if (oldest_write_conflict_snapshot != nullptr) {
      *oldest_write_conflict_snapshot = kMaxSequenceNumber;
    }

    if (empty()) {
      return;
    }
    const SnapshotRecord* s = &list_;
    while (s->next_ != &list_) {
      if (s->next_->number > max_seq) {
        break;
      }
      // Avoid duplicates
      if (ret.empty() || ret.back() != s->next_->number) {
        ret.push_back(s->next_->number);
      }

      if (oldest_write_conflict_snapshot != nullptr &&
          *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
          s->next_->is_write_conflict_boundary) {
        // If this is the first write-conflict boundary snapshot in the list,
        // it is the oldest
        *oldest_write_conflict_snapshot = s->next_->number;
      }

      s = s->next_;
    }
    return;
  }

  // get the sequence number of the most recent snapshot
  SequenceNumber GetNewest() {
    if (empty()) {
      return 0;
    }
    return newest()->number;
  }

  int64_t GetOldestSnapshotTime() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->unix_time;
    }
  }

  int64_t GetOldestSnapshotSequence() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->number;
    }
  }

  uint64_t count() const { return count_; }

 private:
  // This is essentially an atomic shared pointer, which is currently only
  // implemented for the needs of snapshots, but can be extended to support
  // the standard library's shared_ptr<T> if we implement access to the ref
  // block counter and can manipulate it directly
  class SnapshotHolder {
    static_assert((alignof(SnapshotRecord) & (alignof(SnapshotRecord) - 1)) ==
                      0,
                  "Alignment of SnapshotRecord must be a power of 2");
    static_assert(sizeof(uintptr_t) <= sizeof(uint64_t),
                  "Pointer size must be smaller or equal to that of uint64_t");

    // ARMv8 cores have 55 bits in use when pointer authentication is used,
    // leaving 9 bits free, but latest Intel CPUs have 5-level paging which
    // results in 56 bits being used, leaving only 8 bits free.
    static constexpr size_t kFreeUpperBits =
        (std::numeric_limits<uintptr_t>::digits == 64 ? 8 : 0) +
        (CeiledLog2<std::numeric_limits<uint64_t>::max()>::value -
         CeiledLog2<std::numeric_limits<uintptr_t>::max()>::value);
    static constexpr size_t kFreeLowerBits =
        CeiledLog2<alignof(SnapshotRecord)>::value - 1;

    // We need at least 9 free bits to have a menaingful improvement over simple
    // mutual exclusion, otherwise we'll have too much cache-line bouncing due
    // to failed CASs when moving references stored on the pointer to the type's
    // reference counter (we'll also overflow the counter too quickly into the
    // pointer value, depending on the amount of concurrent readers and cores
    // used on the specific machine)
    static constexpr size_t kRefCtrBitsMin = 9;
    static constexpr size_t kRefCtrBitsCount = kFreeUpperBits + kFreeLowerBits;

    static_assert(kRefCtrBitsCount >= kRefCtrBitsMin,
                  "Not enough free bits to store the reference counter");

    // The mask for obtaining the references stored in the pointer's free bits
    static constexpr size_t kRefCtrMask = ((1ul << kRefCtrBitsCount) - 1);
    // How many references we keep on the pointer before we move them to the
    // reference counter of the enclosed type
    static constexpr uint32_t kRefCtrBatchSize =
        kRefCtrBitsCount <= 32 ? (kRefCtrMask + 1) >> 1 : 0x80000000;

    // The mask and shift amount needed to obtain the actual pointer value from
    // the compound pointer+counter
    static constexpr uint64_t kRefCtrPtrShift = kFreeUpperBits;
    static constexpr uint64_t kRefCtrPtrMask =
        std::numeric_limits<uint64_t>::max() ^ ((1ul << kFreeLowerBits) - 1);

    // Initial reference count set in the enclosed type's reference counter to
    // prevent it from being freed from under us when there are still active
    // references stored on the pointer
    static constexpr uint32_t kRefCtrInitValue = 0xffffffff;

   public:
    ~SnapshotHolder() { reset(); }

    SnapshotRecord* reset(SnapshotRecord* s = nullptr);

    SnapshotRecord* ref();

    // REQUIRES: DB mutex held
    inline SnapshotRecord* get() {
      return from_intptr(snapshot_.load(std::memory_order_acquire));
    }

   private:
    static inline SnapshotRecord* from_intptr(int64_t n) {
      return reinterpret_cast<SnapshotRecord*>((n >> kRefCtrPtrShift) &
                                               kRefCtrPtrMask);
    }

    static inline int64_t to_intptr(const SnapshotRecord* s, uint16_t n = 0) {
      return static_cast<int64_t>(
          ((reinterpret_cast<uint64_t>(s) & kRefCtrPtrMask)
           << kRefCtrPtrShift) |
          n);
    }

    std::atomic<int64_t> snapshot_{0};
  };

  SnapshotHolder last_snapshot_;

  // Dummy head of doubly-linked list of snapshots
  SnapshotRecord list_;
  uint64_t count_;
};

}  // namespace ROCKSDB_NAMESPACE
