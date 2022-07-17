#pragma once

#include <byteswap.h>  // bswap_64()
#include <endian.h>    // __BYTE_ORDER __LITTLE_ENDIAN

#include <cassert>
#include <stdio.h>

#include "util/murmurhash.h"

namespace ROCKSDB_NAMESPACE {
class ObjectKey : public std::string {
 public:
  static size_t *Swap(size_t &value) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    value = bswap_64(value);  // Compiler builtin GCC/Clang
#endif
    return &value;
  };
  ObjectKey(size_t val)
      : std::string((const char *)Swap(val), sizeof(size_t)){};
  ObjectKey() : std::string() {};
  size_t val() const {
    if (size()) {
      size_t tmp = *(const size_t *) data();
      return *Swap(tmp);
    } else {
      return 0;
    }
  }	   
};

struct LevelFileId {
  LevelFileId() : id_(-1), level_(-1) {}
  bool operator==(const LevelFileId &sec) const {
    return id_ == sec.id_ && level_ == sec.level_;
  };
  int id_;
  int level_;
};

struct ObjectEntry {
  ObjectKey key_;
  size_t start_location_in_bytes_;
  size_t size_in_bytes_;
};

struct ObjectLocation {
  ObjectLocation() : offset_(0), size_(0) {}
  ObjectLocation(int offset, int size) : offset_(offset), size_(size) {}
  LevelFileId file_id_;
  int offset_;
  int size_;
};

typedef std::vector<ObjectEntry> IndexInput;

class Filter {
 protected:
  Filter() {}
  virtual void SaveExtraData(std::string &to) const = 0;
  Filter(const char *&from);

 public:
  enum FilterType { BLOOM, SPDB_HASH };
  virtual ~Filter(){};
  static Filter *Build(const char *&from);
  static Filter *Build(const IndexInput &, size_t false_pos_rate);

  void Save(std::string &to) const;
  virtual FilterType Type() const = 0;
  
  static const size_t alignment_ = 1;
  virtual void FindPossibleLocation(
      const ObjectKey &key,
      std::list<ObjectLocation> &possible_location) const = 0;
  virtual void FindPossibleLocation(
      size_t object_sign, const std::vector<size_t> &hashes,
      std::list<ObjectLocation> &possible_locations) const = 0;

 public:
  static bool ObjectTooLarge(size_t data_size_in_bytes) {
    return data_size_in_bytes / alignment >= 16;
  }  // we have 4 bits for size
  static bool DistanceTooLarge(size_t distance_in_bytes) {
    return distance_in_bytes /alignment >= 256;
  }  // we have 8 bits for location
 public:
  static size_t CalcHash(const ObjectKey &key, uint seed) {
    return MurmurHash(key.data(), key.size(), predef_seeds[seed]); 
  }
  static size_t Signature(const ObjectKey &key) {
    return CalcHash(key, max_hashes);
  }
};

static inline uint32_t ComputeLocation(const ObjectKey &key, uint hash_num,
				       uint hash_base, uint max_size) {
  return Filter::CalcHash(key, hash_num + hash_base) % max_size;
}


class SpdbHashFilter : public Filter {
 public:
  FilterType Type() const override { return SPDB_HASH; }
  SpdbHashFilter(const char *&from);
  virtual ~SpdbHashFilter(){};

  inline virtual void FindPossibleLocation(
      const ObjectKey &key,
      std::list<ObjectLocation> &possible_locations) const override;
  inline virtual void FindPossibleLocation(
      size_t signature, const std::vector<size_t> &hashes,
      std::list<ObjectLocation> &possible_locations) const override;

  static SpdbHashFilter *Build(const IndexInput &,
                                 size_t false_positive_rate);

 private:
  SpdbHashFilter(size_t size, size_t hash_base) : hash_base_(hash_base), data_(size){};

  virtual void SaveExtraData(std::string &to) const override;
  size_t HashSize() const { return data_.size(); }

  static size_t BuildSignature(size_t hash_val) {
    // keep zero for empty entry ?? needed ??? who cares !!
    return hash_val % ((1 << 20) - 1) + 1;
  }
  static size_t BuildSignature(const ObjectKey &key) {
    return BuildSignature(Filter::Signature(key));
  }
  void Add(uint index, const ObjectKey &key, uint size, uint location);

 private:
  struct FilterEntry {
    uint32_t signature_ : 20;
    uint32_t location_ : 8;
    uint32_t size_ : 4;
  };
  static const size_t spdb_hashes_count_ = 3;
  size_t hash_base_;
  std::vector<FilterEntry> data_;
};

inline void SpdbHashFilter::FindPossibleLocation(
    const ObjectKey &key, std::list<ObjectLocation> &possible_locations) const {
  auto signature = buildSignature(key);
  std::vector<size_t> hashVal(k_nCuckooHashes);

  for (size_t i = 0; i < k_nCuckooHashes; i++) {
    hashVal[i] = computeLocation(key, i, hashBase_, data_.size());
  }
  return FindPossibleLocation(signature, hashVal, possible_locations);
}

inline void SpdbHashFilter::FindPossibleLocation(
    size_t signature, const std::vector<size_t> &hashes,
    std::list<ObjectLocation> &possible_locations) const {
  for (auto h : hashes) {    
    if (data_[h].signature_ == signature) {
      possible_locations.push_back(
          ObjectLocation(data_[h].location_, data_[h].size_));
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
