//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/snapshot_impl.h"

#include <atomic>

#include "rocksdb/db.h"
#include "rocksdb/snapshot.h"

namespace ROCKSDB_NAMESPACE {

ManagedSnapshot::ManagedSnapshot(DB* db) : db_(db),
                                           snapshot_(db->GetSnapshot()) {}

ManagedSnapshot::ManagedSnapshot(DB* db, const Snapshot* _snapshot)
    : db_(db), snapshot_(_snapshot) {}

ManagedSnapshot::~ManagedSnapshot() {
  if (snapshot_) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

const Snapshot* ManagedSnapshot::snapshot() { return snapshot_;}

bool SnapshotList::RefCached(SnapshotImpl* s, SequenceNumber seq) {
  SnapshotRecord* record = last_snapshot_.ref();

  if (record == nullptr) {
    return false;
  }

  s->list_ = this;
  s->number_ = record->number;
  s->record_.store(record);

  if (record->number != seq ||
      (s->is_write_conflict_boundary_ && !record->is_write_conflict_boundary)) {
    return false;
  }

  return true;
}

bool SnapshotList::Unref(const SnapshotImpl* s) {
  SnapshotRecord* record = s->record_.load(std::memory_order_acquire);

  if (record == nullptr) {
    return false;
  }

  assert(s->list_ == this);
  assert(record != nullptr);

  if (record->unref() != 0) {
    const_cast<SnapshotImpl*>(s)->list_ = nullptr;
    const_cast<SnapshotImpl*>(s)->record_.store(nullptr);

    return false;
  }

  return true;
}

// REQUIRES: DB mutex held
bool SnapshotList::Delete(const SnapshotImpl* s) {
  const SnapshotRecord* record = s->record_.load(std::memory_order_acquire);

  assert(s->list_ == this);
  assert(record != nullptr);

  const_cast<SnapshotImpl*>(s)->list_ = nullptr;
  const_cast<SnapshotImpl*>(s)->record_.store(nullptr);

  const SnapshotRecord* prev = record->prev_;

  record->prev_->next_ = record->next_;
  record->next_->prev_ = record->prev_;
  count_--;
  delete record;

  return prev == &list_;
}

// REQUIRES: DB mutex held
bool SnapshotList::ResetCached() {
  SnapshotRecord* record = last_snapshot_.reset();
  if (record == nullptr) {
    return false;
  }

  SnapshotImpl dummy(false);
  dummy.list_ = this;
  dummy.record_.store(record, std::memory_order_relaxed);

  // Delete from list if we were holding the last reference
  return Delete(&dummy);
}

// REQUIRES: DB mutex held
bool SnapshotList::New(SnapshotImpl* s, SequenceNumber seq, int64_t unix_time) {
  assert(s->list_ == nullptr);
  assert(s->record_.load(std::memory_order_acquire) == nullptr);

  SnapshotRecord* record =
      last_snapshot_.get();  // TODO: yuval - why do we need this? just create a
                             // new record and add it to the list.

  bool was_oldest_snapshot_released = false;
  if (record == nullptr || record->number != seq) {
    record = new SnapshotRecord(seq, unix_time, s->is_write_conflict_boundary_);
    record->next_ = &list_;
    record->prev_ = list_.prev_;
    record->prev_->next_ = record;
    record->next_->prev_ = record;
    count_++;

    if (SnapshotRecord* old = last_snapshot_.reset(record)) {
      SnapshotImpl dummy(false);
      dummy.list_ = this;
      dummy.record_.store(old, std::memory_order_relaxed);

      // Delete from list if we were holding the last reference
      was_oldest_snapshot_released = Delete(&dummy);
    }
  } else if (s->is_write_conflict_boundary_ &&  // this means the cached
                                                // snapshot fits in seq but not
                                                // with
                                                // is_write_conflict_boundary
             !record->is_write_conflict_boundary) {
    record->is_write_conflict_boundary = true;
  }
  record->ref();

  s->list_ = this;
  s->number_ = record->number;
  s->record_.store(record, std::memory_order_release);

  return was_oldest_snapshot_released;
}

SnapshotRecord* SnapshotList::SnapshotHolder::reset(SnapshotRecord* record) {
  if (record != nullptr) {
    record->ref(kRefCtrInitValue);
  }
  const int64_t olds =
      intptr_record_.exchange(to_intptr(record), std::memory_order_release);
  SnapshotRecord* old = from_intptr(olds);
  if (old != nullptr) {
    // Truncating here is OK, as we can never have that many active readers
    // anyway (can only happen on 32-bit systems, where we use all of the upper
    // 32-bits, and with types that have an alignment bigger than 1, but we
    // limit kRefCtrBatchSize to 31-bits so it never exceeds kRefCtrInitValue).
    const uint32_t active = uint32_t(olds & kRefCtrMask);
    if (old->unref(kRefCtrInitValue - active) == 0) {
      return old;
    }
  }

  return nullptr;
}

SnapshotRecord* SnapshotList::SnapshotHolder::ref() {
  const int64_t cur_s = intptr_record_.fetch_add(1, std::memory_order_acquire);
  SnapshotRecord* snapshot = from_intptr(cur_s);

  if (snapshot != nullptr) {
    // Move references to the internal snapshot counter every kRefCtrBatchSize
    if ((cur_s & kRefCtrMask) > kRefCtrBatchSize) {
      snapshot->ref(kRefCtrBatchSize);
      int64_t new_s = intptr_record_.load(std::memory_order_acquire);
      for (;;) {
        const size_t active = new_s & kRefCtrMask;
        const SnapshotRecord* nsnap = from_intptr(new_s);

        if (nsnap != snapshot || active <= kRefCtrBatchSize) {
          snapshot->unref(kRefCtrBatchSize);
          break;
        }

        const int64_t updated = to_intptr(snapshot, active - kRefCtrBatchSize);

        if (intptr_record_.compare_exchange_weak(new_s, updated,
                                                 std::memory_order_release)) {
          break;
        }
      }
    }
  }

  return snapshot;
}

}  // namespace ROCKSDB_NAMESPACE
