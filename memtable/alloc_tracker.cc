//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>

#include "memory/allocator.h"
#include "memory/arena.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

AllocTracker::AllocTracker(WriteBufferManager* write_buffer_manager)
    : write_buffer_manager_(write_buffer_manager), bytes_allocated_(0) {}

AllocTracker::~AllocTracker() { FreeMem(); }

void AllocTracker::Allocate(size_t bytes) {
  assert(write_buffer_manager_ != nullptr);
  assert(state_ == State::kAllocating);

  if (state_ == State::kAllocating) {
    if (ShouldUpdateWriteBufferManager()) {
      bytes_allocated_.fetch_add(bytes, std::memory_order_relaxed);
      write_buffer_manager_->ReserveMem(bytes);
    }
  }
}

void AllocTracker::DoneAllocating() {
  assert(write_buffer_manager_ != nullptr);
  assert(state_ == State::kAllocating);

  if (state_ == State::kAllocating) {
    if (ShouldUpdateWriteBufferManager()) {
      write_buffer_manager_->ScheduleFreeMem(
          bytes_allocated_.load(std::memory_order_relaxed));
    } else {
      assert(bytes_allocated_.load(std::memory_order_relaxed) == 0);
    }
    state_ = State::kDoneAllocating;
  }
}

void AllocTracker::FreeMemStarted() {
  assert(write_buffer_manager_ != nullptr);
  assert(state_ == State::kDoneAllocating);

  if (state_ == State::kDoneAllocating) {
    if (ShouldUpdateWriteBufferManager()) {
      write_buffer_manager_->FreeMemBegin(
          bytes_allocated_.load(std::memory_order_relaxed));
    }
    state_ = State::kFreeMemStarted;
  }
}

void AllocTracker::FreeMemAborted() {
  assert(write_buffer_manager_ != nullptr);
  // May be called without actually starting to free memory
  assert((state_ == State::kDoneAllocating) ||
         (state_ == State::kFreeMemStarted));

  if (state_ == State::kFreeMemStarted) {
    if (ShouldUpdateWriteBufferManager()) {
      write_buffer_manager_->FreeMemAborted(
          bytes_allocated_.load(std::memory_order_relaxed));
    }
    state_ = State::kDoneAllocating;
  }
}

void AllocTracker::FreeMem() {
  if (state_ == State::kAllocating) {
    DoneAllocating();
  }

  // This is necessary so that the WBM will not decrease the memory being
  // freed twice in case memory freeing was aborted and then freed via this
  // call
  if (state_ == State::kDoneAllocating) {
    FreeMemStarted();
  }

  if (state_ == State::kFreeMemStarted) {
    if (ShouldUpdateWriteBufferManager()) {
      write_buffer_manager_->FreeMem(
          bytes_allocated_.load(std::memory_order_relaxed));
    } else {
      assert(bytes_allocated_.load(std::memory_order_relaxed) == 0);
    }
  }

  state_ = State::kFreed;
}

}  // namespace ROCKSDB_NAMESPACE
