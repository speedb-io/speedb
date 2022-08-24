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

#pragma once
#include <assert.h>
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <type_traits>

#include "memory/allocator.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

template <class Comparator>
class SpdbSortedList {
 private:
  struct Node;
  struct Splice;

 public:
  using DecodedKey =
      typename std::remove_reference<Comparator>::type::DecodedType;

  static const uint16_t kMaxPossibleHeight = 32;

  explicit SpdbSortedList(Comparator cmp, Allocator* allocator,
                          int32_t max_height = 12,
                          int32_t branching_factor = 4);
  SpdbSortedList(const SpdbSortedList&) = delete;
  SpdbSortedList& operator=(const SpdbSortedList&) = delete;

  char* AllocateSpdbItem(size_t key_size, size_t header_size,
                         char** spdb_sorted_key);

  Splice* AllocateSplice();

  Splice* AllocateSpliceOnHeap();

  bool Insert(const char* key, bool concurrently);

  template <bool UseCAS>
  bool Insert(const char* key, Splice* splice);

  class Iterator {
   public:
    explicit Iterator(const SpdbSortedList* list);

    void SetList(const SpdbSortedList* list);

    void SetSeek(const char* key);

    bool Valid() const;

    const char* key() const;

    void Next();

    void Prev();

    void Seek(const char* target);

    void SeekForPrev(const char* target);

    void RandomSeek();

    void SeekToFirst();

    void SeekToLast();

   private:
    const SpdbSortedList* list_;
    Node* node_;
  };

 private:
  const uint16_t kMaxHeight_;
  const uint16_t kBranching_;
  const uint32_t kScaledInverseBranching_;

  Allocator* const allocator_;
  Comparator const compare_;
  Node* const head_;

  std::atomic<int> max_height_;

  Splice* seq_splice_;

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  int RandomHeight();

  Node* AllocateNode(size_t key_size, int height);

  bool Equal(const char* a, const char* b) const {
    return (compare_(a, b) == 0);
  }

  bool LessThan(const char* a, const char* b) const {
    return (compare_(a, b) < 0);
  }

  bool KeyIsAfterNode(const char* key, Node* n) const;
  bool KeyIsAfterNode(const DecodedKey& key, Node* n) const;

  Node* FindGreaterOrEqual(const char* key) const;

  Node* FindLessThan(const char* key, Node** prev = nullptr) const;

  Node* FindLessThan(const char* key, Node** prev, Node* root, int top_level,
                     int bottom_level) const;

  Node* FindLast() const;

  Node* FindRandomEntry() const;

  template <bool prefetch_before>
  void FindSpliceForLevel(const DecodedKey& key, Node* before, Node* after,
                          int level, Node** out_prev, Node** out_next);

  void RecomputeSpliceLevels(const DecodedKey& key, Splice* splice,
                             int recompute_level);
};

template <class Comparator>
struct SpdbSortedList<Comparator>::Splice {
  int height_ = 0;
  Node** prev_;
  Node** next_;
};

template <class Comparator>
struct SpdbSortedList<Comparator>::Node {
  void StashHeight(const int height) {
    assert(sizeof(int) <= sizeof(next_[0]));
    memcpy(static_cast<void*>(&next_[0]), &height, sizeof(int));
  }

  int UnstashHeight() const {
    int rv;
    memcpy(&rv, &next_[0], sizeof(int));
    return rv;
  }

  const char* Key() const { return reinterpret_cast<const char*>(&next_[1]); }

  Node* Next(int n) {
    assert(n >= 0);
    return ((&next_[0] - n)->load(std::memory_order_acquire));
  }

  void SetNext(int n, Node* x) {
    assert(n >= 0);
    (&next_[0] - n)->store(x, std::memory_order_release);
  }

  bool CASNext(int n, Node* expected, Node* x) {
    assert(n >= 0);
    return (&next_[0] - n)->compare_exchange_strong(expected, x);
  }

  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return (&next_[0] - n)->load(std::memory_order_relaxed);
  }

  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    (&next_[0] - n)->store(x, std::memory_order_relaxed);
  }

  void InsertAfter(Node* prev, int level) {
    NoBarrier_SetNext(level, prev->NoBarrier_Next(level));
    prev->SetNext(level, this);
  }

 private:
  std::atomic<Node*> next_[1];
};

template <class Comparator>
inline SpdbSortedList<Comparator>::Iterator::Iterator(
    const SpdbSortedList* list) {
  SetList(list);
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::SetList(
    const SpdbSortedList* list) {
  list_ = list;
  node_ = nullptr;
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::SetSeek(const char* key) {
  node_ = reinterpret_cast<Node*>(const_cast<char*>(key)) - 1;
}

template <class Comparator>
inline bool SpdbSortedList<Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <class Comparator>
inline const char* SpdbSortedList<Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->Key();
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->Key());
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::Seek(const char* target) {
  node_ = list_->FindGreaterOrEqual(target);
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::SeekForPrev(
    const char* target) {
  Seek(target);
  if (!Valid()) {
    SeekToLast();
  }
  while (Valid() && list_->LessThan(target, key())) {
    Prev();
  }
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::RandomSeek() {
  node_ = list_->FindRandomEntry();
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <class Comparator>
inline void SpdbSortedList<Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
int SpdbSortedList<Comparator>::RandomHeight() {
  auto rnd = Random::GetTLSInstance();

  int height = 1;
  while (height < kMaxHeight_ && height < kMaxPossibleHeight &&
         rnd->Next() < kScaledInverseBranching_) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight_);
  assert(height <= kMaxPossibleHeight);
  return height;
}

template <class Comparator>
bool SpdbSortedList<Comparator>::KeyIsAfterNode(const char* key,
                                                Node* n) const {
  assert(n != head_);
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

template <class Comparator>
bool SpdbSortedList<Comparator>::KeyIsAfterNode(const DecodedKey& key,
                                                Node* n) const {
  assert(n != head_);
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

template <class Comparator>
typename SpdbSortedList<Comparator>::Node*
SpdbSortedList<Comparator>::FindGreaterOrEqual(const char* key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  Node* last_bigger = nullptr;
  const DecodedKey key_decoded = compare_.decode_key(key);
  while (true) {
    Node* next = x->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
    }
    assert(x == head_ || next == nullptr || KeyIsAfterNode(next->Key(), x));
    assert(x == head_ || KeyIsAfterNode(key_decoded, x));
    int cmp = (next == nullptr || next == last_bigger)
                  ? 1
                  : compare_(next->Key(), key_decoded);
    if (cmp == 0 || (cmp > 0 && level == 0)) {
      return next;
    } else if (cmp < 0) {
      x = next;
    } else {
      last_bigger = next;
      level--;
    }
  }
}

template <class Comparator>
typename SpdbSortedList<Comparator>::Node*
SpdbSortedList<Comparator>::FindLessThan(const char* key, Node** prev) const {
  return FindLessThan(key, prev, head_, GetMaxHeight(), 0);
}

template <class Comparator>
typename SpdbSortedList<Comparator>::Node*
SpdbSortedList<Comparator>::FindLessThan(const char* key, Node** prev,
                                         Node* root, int top_level,
                                         int bottom_level) const {
  assert(top_level > bottom_level);
  int level = top_level - 1;
  Node* x = root;
  Node* last_not_after = nullptr;
  const DecodedKey key_decoded = compare_.decode_key(key);
  while (true) {
    assert(x != nullptr);
    Node* next = x->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
    }
    assert(x == head_ || next == nullptr || KeyIsAfterNode(next->Key(), x));
    assert(x == head_ || KeyIsAfterNode(key_decoded, x));
    if (next != last_not_after && KeyIsAfterNode(key_decoded, next)) {
      assert(next != nullptr);
      x = next;
    } else {
      if (prev != nullptr) {
        prev[level] = x;
      }
      if (level == bottom_level) {
        return x;
      } else {
        last_not_after = next;
        level--;
      }
    }
  }
}

template <class Comparator>
typename SpdbSortedList<Comparator>::Node*
SpdbSortedList<Comparator>::FindLast() const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <class Comparator>
typename SpdbSortedList<Comparator>::Node*
SpdbSortedList<Comparator>::FindRandomEntry() const {
  Node *x = head_, *scan_node = nullptr, *limit_node = nullptr;

  std::vector<Node*> lvl_nodes;
  Random* rnd = Random::GetTLSInstance();
  int level = GetMaxHeight() - 1;

  while (level >= 0) {
    lvl_nodes.clear();
    scan_node = x;
    while (scan_node != limit_node) {
      lvl_nodes.push_back(scan_node);
      scan_node = scan_node->Next(level);
    }
    uint32_t rnd_idx = rnd->Next() % lvl_nodes.size();
    x = lvl_nodes[rnd_idx];
    if (rnd_idx + 1 < lvl_nodes.size()) {
      limit_node = lvl_nodes[rnd_idx + 1];
    }
    level--;
  }
  return x == head_ ? head_->Next(0) : x;
}

template <class Comparator>
SpdbSortedList<Comparator>::SpdbSortedList(const Comparator cmp,
                                           Allocator* allocator,
                                           int32_t max_height,
                                           int32_t branching_factor)
    : kMaxHeight_(static_cast<uint16_t>(max_height)),
      kBranching_(static_cast<uint16_t>(branching_factor)),
      kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),
      allocator_(allocator),
      compare_(cmp),
      head_(AllocateNode(0, max_height)),
      max_height_(1),
      seq_splice_(AllocateSplice()) {
  assert(max_height > 0 && kMaxHeight_ == static_cast<uint32_t>(max_height));
  assert(branching_factor > 1 &&
         kBranching_ == static_cast<uint32_t>(branching_factor));
  assert(kScaledInverseBranching_ > 0);

  for (int i = 0; i < kMaxHeight_; ++i) {
    head_->SetNext(i, nullptr);
  }
}

template <class Comparator>
char* SpdbSortedList<Comparator>::AllocateSpdbItem(size_t key_size,
                                                   size_t header_size,
                                                   char** spdb_sorted_key) {
  int height = RandomHeight();
  auto prefix = header_size + sizeof(std::atomic<Node*>) * (height - 1);

  // prefix is space for the height - 1 pointers that we store before
  // the Node instance (next_[-(height - 1) .. -1]).  Node starts at
  // raw + prefix, and holds the bottom-mode (level 0) spdbsortedlist pointer
  // next_[0].  key_size is the bytes for the key, which comes just after
  // the Node.
  char* raw = allocator_->AllocateAligned(prefix + sizeof(Node) + key_size);
  Node* x = reinterpret_cast<Node*>(raw + prefix);
  x->StashHeight(height);
  *spdb_sorted_key = const_cast<char*>(x->Key());
  return raw;
}

template <class Comparator>
typename SpdbSortedList<Comparator>::Node*
SpdbSortedList<Comparator>::AllocateNode(size_t key_size, int height) {
  auto prefix = sizeof(std::atomic<Node*>) * (height - 1);
  char* raw = allocator_->AllocateAligned(prefix + sizeof(Node) + key_size);
  Node* x = reinterpret_cast<Node*>(raw + prefix);
  x->StashHeight(height);
  return x;
}

template <class Comparator>
typename SpdbSortedList<Comparator>::Splice*
SpdbSortedList<Comparator>::AllocateSplice() {
  // size of prev_ and next_
  size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
  char* raw = allocator_->AllocateAligned(sizeof(Splice) + array_size * 2);
  Splice* splice = reinterpret_cast<Splice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<Node**>(raw + sizeof(Splice));
  splice->next_ = reinterpret_cast<Node**>(raw + sizeof(Splice) + array_size);
  return splice;
}

template <class Comparator>
typename SpdbSortedList<Comparator>::Splice*
SpdbSortedList<Comparator>::AllocateSpliceOnHeap() {
  size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
  char* raw = new char[sizeof(Splice) + array_size * 2];
  Splice* splice = reinterpret_cast<Splice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<Node**>(raw + sizeof(Splice));
  splice->next_ = reinterpret_cast<Node**>(raw + sizeof(Splice) + array_size);
  return splice;
}

template <class Comparator>
bool SpdbSortedList<Comparator>::Insert(const char* key, bool concurrently) {
  Splice splice;
  if (concurrently) {
    Node* prev[kMaxPossibleHeight];
    Node* next[kMaxPossibleHeight];
    splice.prev_ = prev;
    splice.next_ = next;
    return Insert<true>(key, &splice);
  } else {
    return Insert<false>(key, seq_splice_);
  }
}

template <class Comparator>
template <bool prefetch_before>
void SpdbSortedList<Comparator>::FindSpliceForLevel(const DecodedKey& key,
                                                    Node* before, Node* after,
                                                    int level, Node** out_prev,
                                                    Node** out_next) {
  while (true) {
    Node* next = before->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
    }
    if (prefetch_before == true) {
      if (next != nullptr && level > 0) {
        PREFETCH(next->Next(level - 1), 0, 1);
      }
    }
    assert(before == head_ || next == nullptr ||
           KeyIsAfterNode(next->Key(), before));
    assert(before == head_ || KeyIsAfterNode(key, before));
    if (next == after || !KeyIsAfterNode(key, next)) {
      // found it
      *out_prev = before;
      *out_next = next;
      return;
    }
    before = next;
  }
}

template <class Comparator>
void SpdbSortedList<Comparator>::RecomputeSpliceLevels(const DecodedKey& key,
                                                       Splice* splice,
                                                       int recompute_level) {
  assert(recompute_level > 0);
  assert(recompute_level <= splice->height_);
  for (int i = recompute_level - 1; i >= 0; --i) {
    FindSpliceForLevel<true>(key, splice->prev_[i + 1], splice->next_[i + 1], i,
                             &splice->prev_[i], &splice->next_[i]);
  }
}

template <class Comparator>
template <bool UseCAS>
bool SpdbSortedList<Comparator>::Insert(const char* key, Splice* splice) {
  Node* x = reinterpret_cast<Node*>(const_cast<char*>(key)) - 1;
  const DecodedKey key_decoded = compare_.decode_key(key);
  int height = x->UnstashHeight();
  assert(height >= 1 && height <= kMaxHeight_);

  int max_height = max_height_.load(std::memory_order_relaxed);
  while (height > max_height) {
    if (max_height_.compare_exchange_weak(max_height, height)) {
      // successfully updated it
      max_height = height;
      break;
    }
    // else retry, possibly exiting the loop because somebody else
    // increased it
  }
  assert(max_height <= kMaxPossibleHeight);

  int recompute_height = 0;
  if (splice->height_ < max_height) {
    // Either splice has never been used or max_height has grown since
    // last use.  We could potentially fix it in the latter case, but
    // that is tricky.
    splice->prev_[max_height] = head_;
    splice->next_[max_height] = nullptr;
    splice->height_ = max_height;
    recompute_height = max_height;
  } else {
    while (recompute_height < max_height) {
      if (splice->prev_[recompute_height]->Next(recompute_height) !=
          splice->next_[recompute_height]) {
        ++recompute_height;
      } else if (splice->prev_[recompute_height] != head_ &&
                 !KeyIsAfterNode(key_decoded,
                                 splice->prev_[recompute_height])) {
        recompute_height = max_height;
      } else if (KeyIsAfterNode(key_decoded, splice->next_[recompute_height])) {
        recompute_height = max_height;
      } else {
        // this level brackets the key, we won!
        break;
      }
    }
  }
  assert(recompute_height <= max_height);
  if (recompute_height > 0) {
    RecomputeSpliceLevels(key_decoded, splice, recompute_height);
  }

  bool splice_is_valid = true;
  if (UseCAS) {
    for (int i = 0; i < height; ++i) {
      while (true) {
        // Checking for duplicate keys on the level 0 is sufficient
        if (UNLIKELY(i == 0 && splice->next_[i] != nullptr &&
                     compare_(x->Key(), splice->next_[i]->Key()) >= 0)) {
          // duplicate key
          return false;
        }
        if (UNLIKELY(i == 0 && splice->prev_[i] != head_ &&
                     compare_(splice->prev_[i]->Key(), x->Key()) >= 0)) {
          // duplicate key
          return false;
        }
        assert(splice->next_[i] == nullptr ||
               compare_(x->Key(), splice->next_[i]->Key()) < 0);
        assert(splice->prev_[i] == head_ ||
               compare_(splice->prev_[i]->Key(), x->Key()) < 0);
        x->NoBarrier_SetNext(i, splice->next_[i]);
        if (splice->prev_[i]->CASNext(i, splice->next_[i], x)) {
          // success
          break;
        }
        // CAS failed, we need to recompute prev and next. It is unlikely
        // to be helpful to try to use a different level as we redo the
        // search, because it should be unlikely that lots of nodes have
        // been inserted between prev[i] and next[i]. No point in using
        // next[i] as the after hint, because we know it is stale.
        FindSpliceForLevel<false>(key_decoded, splice->prev_[i], nullptr, i,
                                  &splice->prev_[i], &splice->next_[i]);

        // Since we've narrowed the bracket for level i, we might have
        // violated the Splice constraint between i and i-1.  Make sure
        // we recompute the whole thing next time.
        if (i > 0) {
          splice_is_valid = false;
        }
      }
    }
  } else {
    for (int i = 0; i < height; ++i) {
      if (i >= recompute_height &&
          splice->prev_[i]->Next(i) != splice->next_[i]) {
        FindSpliceForLevel<false>(key_decoded, splice->prev_[i], nullptr, i,
                                  &splice->prev_[i], &splice->next_[i]);
      }
      // Checking for duplicate keys on the level 0 is sufficient
      if (UNLIKELY(i == 0 && splice->next_[i] != nullptr &&
                   compare_(x->Key(), splice->next_[i]->Key()) >= 0)) {
        // duplicate key
        return false;
      }
      if (UNLIKELY(i == 0 && splice->prev_[i] != head_ &&
                   compare_(splice->prev_[i]->Key(), x->Key()) >= 0)) {
        // duplicate key
        return false;
      }
      assert(splice->next_[i] == nullptr ||
             compare_(x->Key(), splice->next_[i]->Key()) < 0);
      assert(splice->prev_[i] == head_ ||
             compare_(splice->prev_[i]->Key(), x->Key()) < 0);
      assert(splice->prev_[i]->Next(i) == splice->next_[i]);
      x->NoBarrier_SetNext(i, splice->next_[i]);
      splice->prev_[i]->SetNext(i, x);
    }
  }
  if (splice_is_valid) {
    for (int i = 0; i < height; ++i) {
      splice->prev_[i] = x;
    }
    assert(splice->prev_[splice->height_] == head_);
    assert(splice->next_[splice->height_] == nullptr);
    for (int i = 0; i < splice->height_; ++i) {
      assert(splice->next_[i] == nullptr ||
             compare_(key, splice->next_[i]->Key()) < 0);
      assert(splice->prev_[i] == head_ ||
             compare_(splice->prev_[i]->Key(), key) <= 0);
      assert(splice->prev_[i + 1] == splice->prev_[i] ||
             splice->prev_[i + 1] == head_ ||
             compare_(splice->prev_[i + 1]->Key(), splice->prev_[i]->Key()) <
                 0);
      assert(splice->next_[i + 1] == splice->next_[i] ||
             splice->next_[i + 1] == nullptr ||
             compare_(splice->next_[i]->Key(), splice->next_[i + 1]->Key()) <
                 0);
    }
  } else {
    splice->height_ = 0;
  }
  return true;
}

}  // namespace ROCKSDB_NAMESPACE
