
#include "spdb_hash.h"

namespace ROCKSDB_NAMESPACE {

#define BUFFER_MUL_SIZE  1024
SpdbHash::SpdbHash(size_t nElements)
    : buffer_(nElements * (BUFFER_MUL_SIZE + BUFFER_MUL_SIZE / spaceInc) / BUFFER_MUL_SIZE), m_hashBase(0) {}

void SpdbHash::Insert(const ObjectEntry *p) {
  if (!Add(p)) {
    // we could not add so we try another values for hash
    ReHash(p);
  }
}


bool SpdbHash::Add(const ObjectEntry *p) {
  std::map<size_t, bool> checked_key;
  return (RecursiveAdd(p, checked_key));
}

bool SpdbHash::RecursiveAdd(const ObjectEntry *entry,
                              std::map<size_t, bool> &checked_key) {
  auto &key = entry->key_;
  uint64_t hashes[n_hashes];
  for (int i = 0; i < n_hashes; i++) {
    auto hash = hashes[i] = ComputeLocation(key, i, m_hashBase, n_max_keys());
    if (buffer_[hashVal] == 0) {
      buffer_[hashVal] = entry;
      return true;
    }
  }

  // no empty was found try with one of the other options
  for (int i = 0; i < n_hashes; i++) {
    auto hashVal = hashes[i];
    if (checkedKey.find(hashVal) == checkedKey.end()) {
      checkedKey.insert(std::make_pair(hashVal, true));
      if (recursiveAdd(buffer_[hashVal], checkedKey)) {
        buffer_[hashVal] = entry;
        return true;
      }
    }
  }
  // dead-end
  return false;
}

void SpdbHash::reHash(const ObjectEntry *p) {
  uint16_t hashBase = m_hashBase + 1;
  size_t newSize = n_maxKeys();
  if (hashBase >= s_try) {
    hashBase = 0;
    newSize = newSize * (1024 + 1024 / spaceInc) / 1024;
    // printf("must increase size from %u to %u (n_keys=%u)!!\n", m_size,
    // newSize, m_nKeys);
  };

  uint safeCount = 0;
  do {
    SpdbHash second(newSize);
    second.m_hashBase = hashBase;
    bool failed = false;
    for (auto &entry : buffer_) {
      if (entry) {
        if (!second.tryToAdd(entry)) {
          failed = true;
          break;
        }
      }
    }
    if (!failed) {
      if (second.tryToAdd(p)) {
        buffer_ = second.buffer_;
        m_hashBase = second.m_hashBase;
        return;
      }
    }
    if (hashBase >= s_try) {
      hashBase = 0;
      newSize = newSize * (1024 + 1024 / spaceInc) / 1024 + 1;
      // printf("must increase size from %lu to %lu !!\n", buffer_.size(),
      // newSize);
    } else {
      hashBase++;
    }
  } while (safeCount++ < s_try * 1024);
  assert(0);
}
}  // namespace ROCKSDB_NAMESPACE
