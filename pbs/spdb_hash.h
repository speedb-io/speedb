#pragma once
#include <map>

#include "filter.h"

namespace ROCKSDB_NAMESPACE {

class SpdbHash {
 public:
  SpdbHash(size_t initial_size);
  ~SpdbHash() {}

 public:
  void Insert(const ObjectEntry *);

 public:
  std::vector<const ObjectEntry *> buffer_;
  uint GetMaxKeys() { return buffer_.size(); }
  uint32_t hash_base_ = 0;
  static const uint32_t hash_num_ = 3;
  static const uint space_ = 16;

 private:
  static const uint32_t try_count_ = 8;
  bool Add(const ObjectEntry *p);
  bool RecursiveAdd(const ObjectEntry *p, std::map<size_t, bool> &checked_key);
  void ReHash(const ObjectEntry *p);
};
}  // namespace ROCKSDB_NAMESPACE
