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

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <mutex>
#include <vector>

#include "db/dbformat.h"
#ifdef SPEEDB_SNAP_OPTIMIZATION
#include "folly/concurrency/AtomicSharedPtr.h"
#endif
#include "rocksdb/db.h"
#include "rocksdb/types.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
class SnapshotImpl : public Snapshot {
 public:
  int64_t unix_time_;
  uint64_t timestamp_;
  // Will this snapshot be used by a Transaction to do write-conflict checking?
  bool is_write_conflict_boundary_;

  SnapshotImpl() {}

  SnapshotImpl(SnapshotImpl* s) {
    number_ = s->number_;
    unix_time_ = s->unix_time_;
    is_write_conflict_boundary_ = s->is_write_conflict_boundary_;
    timestamp_ = s->timestamp_;
  }

#ifdef SPEEDB_SNAP_OPTIMIZATION
  std::atomic_uint64_t refcount = {1};
  std::shared_ptr<SnapshotImpl> cached_snapshot = nullptr;

  struct Deleter {
    inline void operator()(SnapshotImpl* snap) const;
  };
  // Will this snapshot be used by a Transaction to do write-conflict checking?
#endif
  SequenceNumber number_;  // const after creation
  // It indicates the smallest uncommitted data at the time the snapshot was
  // taken. This is currently used by WritePrepared transactions to limit the
  // scope of queries to IsInSnapshot.
  SequenceNumber min_uncommitted_ = kMinUnCommittedSeq;

  int64_t GetUnixTime() const override { return unix_time_; }

  uint64_t GetTimestamp() const override { return timestamp_; }
  SequenceNumber GetSequenceNumber() const override { return number_; }

 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list
  SnapshotImpl* prev_;
  SnapshotImpl* next_;

  SnapshotList* list_;
};

class SnapshotList {
 public:
  mutable std::mutex lock_;
  SystemClock* clock_;
#ifdef SPEEDB_SNAP_OPTIMIZATION
  bool deleteitem_ = false;
  folly::atomic_shared_ptr<SnapshotImpl> last_snapshot_;
#endif
  SnapshotList(SystemClock* clock) {
    clock_ = clock;
    list_.prev_ = &list_;
    list_.next_ = &list_;
    list_.number_ = 0xFFFFFFFFL;  // placeholder marker, for debugging
    // Set all the variables to make UBSAN happy.
    list_.list_ = nullptr;
    list_.unix_time_ = 0;
    list_.timestamp_ = 0;
    list_.is_write_conflict_boundary_ = false;
    count_ = 0;
#ifdef SPEEDB_SNAP_OPTIMIZATION
    last_snapshot_ = nullptr;
#endif
  }
  SnapshotImpl* RefSnapshot([[maybe_unused]] bool is_write_conflict_boundary,
                            [[maybe_unused]] SequenceNumber last_seq) {
#ifdef SPEEDB_SNAP_OPTIMIZATION
    std::shared_ptr<SnapshotImpl> shared_snap = last_snapshot_;
    if (shared_snap && shared_snap->GetSequenceNumber() == last_seq &&
        shared_snap->is_write_conflict_boundary_ ==
            is_write_conflict_boundary) {
      SnapshotImpl* snapshot = new SnapshotImpl;
      clock_->GetCurrentTime(&snapshot->unix_time_)
          .PermitUncheckedError();  // Ignore error
      snapshot->cached_snapshot = shared_snap;
      logical_count_.fetch_add(1);
      shared_snap->refcount.fetch_add(1);
      snapshot->number_ = shared_snap->GetSequenceNumber();
      snapshot->is_write_conflict_boundary_ = is_write_conflict_boundary;
      return snapshot;
    }
#endif
    return nullptr;
  }

  // No copy-construct.
  SnapshotList(const SnapshotList&) = delete;

  bool empty() const {
    assert(list_.next_ != &list_ || 0 == count_);
    return list_.next_ == &list_;
  }
  SnapshotImpl* oldest() const {
    assert(!empty());
    return list_.next_;
  }
  SnapshotImpl* newest() const {
    assert(!empty());
    return list_.prev_;
  }

#ifdef SPEEDB_SNAP_OPTIMIZATION
  SnapshotImpl* NewSnapRef(SnapshotImpl* s) {
    // user snapshot is a reference to the snapshot inside the SnapshotList
    // Unfortunatly right now the snapshot api cannot return shared_ptr to the
    // user so a deep copy should be created
    // s is the original snapshot that is being stored in the SnapshotList
    SnapshotImpl* user_snapshot = new SnapshotImpl(s);
    auto new_last_snapshot =
        std::shared_ptr<SnapshotImpl>(s, SnapshotImpl::Deleter{});
    // may call Deleter
    last_snapshot_ = new_last_snapshot;
    user_snapshot->cached_snapshot = last_snapshot_;
    return user_snapshot;
  }
#endif
  bool UnRefSnapshot([[maybe_unused]] const SnapshotImpl* snapshot) {
#ifdef SPEEDB_SNAP_OPTIMIZATION
    SnapshotImpl* snap = const_cast<SnapshotImpl*>(snapshot);
    logical_count_.fetch_sub(1);
    size_t cnt = snap->cached_snapshot->refcount.fetch_sub(1);
    if (cnt < 2) {
      last_snapshot_.compare_exchange_weak(snap->cached_snapshot, nullptr);
    }
    delete snap;
    if (!deleteitem_) {
      // item has not been deleted from SnapshotList
      return true;
    }
#endif
    return false;
  }

  SnapshotImpl* New(SequenceNumber seq, bool is_write_conflict_boundary,
                    uint64_t ts = std::numeric_limits<uint64_t>::max()) {
    SnapshotImpl* s = new SnapshotImpl;
#ifdef SPEEDB_SNAP_OPTIMIZATION
    std::unique_lock<std::mutex> l(lock_);
    logical_count_.fetch_add(1);
#endif
    clock_->GetCurrentTime(&s->unix_time_)
        .PermitUncheckedError();  // Ignore error
    s->number_ = seq;
    s->timestamp_ = ts;
    s->is_write_conflict_boundary_ = is_write_conflict_boundary;
    s->list_ = this;
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    count_++;
#ifdef SPEEDB_SNAP_OPTIMIZATION
    l.unlock();
    return NewSnapRef(s);
#endif
    return s;
  }

  // Do not responsible to free the object.
  void Delete(const SnapshotImpl* s) {
#ifdef SPEEDB_SNAP_OPTIMIZATION
    std::unique_lock<std::mutex> l(lock_);
    deleteitem_ = false;
#else
    assert(s->list_ == this);
    count_--;
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    delete s;
#endif
  }

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
#ifdef SPEEDB_SNAP_OPTIMIZATION
    std::scoped_lock<std::mutex> l(lock_);
#endif
    std::vector<SequenceNumber>& ret = *snap_vector;
    // So far we have no use case that would pass a non-empty vector
    assert(ret.size() == 0);

    if (oldest_write_conflict_snapshot != nullptr) {
      *oldest_write_conflict_snapshot = kMaxSequenceNumber;
    }

    if (empty()) {
      return;
    }
    const SnapshotImpl* s = &list_;
    while (s->next_ != &list_) {
      if (s->next_->number_ > max_seq) {
        break;
      }
      // Avoid duplicates
      if (ret.empty() || ret.back() != s->next_->number_) {
        ret.push_back(s->next_->number_);
      }

      if (oldest_write_conflict_snapshot != nullptr &&
          *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
          s->next_->is_write_conflict_boundary_) {
        // If this is the first write-conflict boundary snapshot in the list,
        // it is the oldest
        *oldest_write_conflict_snapshot = s->next_->number_;
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
    return newest()->number_;
  }

  int64_t GetOldestSnapshotTime() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->unix_time_;
    }
  }

  int64_t GetOldestSnapshotSequence() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->GetSequenceNumber();
    }
  }

  // How many snapshots in the SnapshotList
  uint64_t count() const { return count_; }
  // How many snapshots in the system included those that created refcount
  uint64_t logical_count() const { return logical_count_; }

  std::atomic_uint64_t logical_count_ = {0};
  uint64_t count_;

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl list_;
};

// All operations on TimestampedSnapshotList must be protected by db mutex.
class TimestampedSnapshotList {
 public:
  explicit TimestampedSnapshotList() = default;

  std::shared_ptr<const SnapshotImpl> GetSnapshot(uint64_t ts) const {
    if (ts == std::numeric_limits<uint64_t>::max() && !snapshots_.empty()) {
      auto it = snapshots_.rbegin();
      assert(it != snapshots_.rend());
      return it->second;
    }
    auto it = snapshots_.find(ts);
    if (it == snapshots_.end()) {
      return std::shared_ptr<const SnapshotImpl>();
    }
    return it->second;
  }

  void GetSnapshots(
      uint64_t ts_lb, uint64_t ts_ub,
      std::vector<std::shared_ptr<const Snapshot>>& snapshots) const {
    assert(ts_lb < ts_ub);
    auto it_low = snapshots_.lower_bound(ts_lb);
    auto it_high = snapshots_.lower_bound(ts_ub);
    for (auto it = it_low; it != it_high; ++it) {
      snapshots.emplace_back(it->second);
    }
  }

  void AddSnapshot(const std::shared_ptr<const SnapshotImpl>& snapshot) {
    assert(snapshot);
    snapshots_.try_emplace(snapshot->GetTimestamp(), snapshot);
  }

  // snapshots_to_release: the container to where the timestamped snapshots will
  // be moved so that it retains the last reference to the snapshots and the
  // snapshots won't be actually released which requires db mutex. The
  // snapshots will be released by caller of ReleaseSnapshotsOlderThan().
  void ReleaseSnapshotsOlderThan(
      uint64_t ts,
      autovector<std::shared_ptr<const SnapshotImpl>>& snapshots_to_release) {
    auto ub = snapshots_.lower_bound(ts);
    for (auto it = snapshots_.begin(); it != ub; ++it) {
      snapshots_to_release.emplace_back(it->second);
    }
    snapshots_.erase(snapshots_.begin(), ub);
  }

 private:
  std::map<uint64_t, std::shared_ptr<const SnapshotImpl>> snapshots_;
};
#ifdef SPEEDB_SNAP_OPTIMIZATION
inline void SnapshotImpl::Deleter::operator()(SnapshotImpl* snap) const {
  if (snap->cached_snapshot == nullptr) {
    std::scoped_lock<std::mutex> l(snap->list_->lock_);
    snap->list_->count_--;
    snap->prev_->next_ = snap->next_;
    snap->next_->prev_ = snap->prev_;
    snap->list_->deleteitem_ = true;
  }
  delete snap;
}
#endif
}  // namespace ROCKSDB_NAMESPACE
