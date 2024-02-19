// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// An example code demonstrating how to use CompactFiles, EventListener,
// and GetColumnFamilyMetaData APIs to implement custom compaction algorithm. 
#include <mutex>
#include <string>
#include <arpa/inet.h>
#include <memory>
#include <chrono>
#include <iomanip>

#include <iostream> 
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/statistics.h"

extern bool gs_debug_prints;

#define USE_GET_SMALLEST 1
#define USE_SINGLE_DELETE 0 

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_delete_range_example";
#else
std::string kDBPath = "/tmp/rocksdb_delete_range_example";
#endif

uint64_t num_popped = 0U;

struct PerPriority
{
  uint32_t last_read = 1;
  uint32_t last_written = 1;
  bool empty() {return last_read == last_written;}
};

static constexpr int kNPriorities= 32;

// Priorities are 1-32, priority 0 (smallest) is left for DeleteRange()
PerPriority debug_info[1 + kNPriorities];
bool NoValBefore(uint p) {
  for (uint i = 0; i < p; i++) {
    if (!debug_info[i].empty())
      return false;
  }
  return true;
}

class CountingComparator : public Comparator {
 public:
  virtual const char* Name() const override { return "UdiComparator"; }
  virtual int Compare(const Slice& a, const Slice& b) const override {
    ++num_comparisons;
    return BytewiseComparator()->Compare(a, b);
  }
  virtual void FindShortestSeparator(std::string* s,
                                     const Slice& l) const override {
    BytewiseComparator()->FindShortestSeparator(s, l);
  }
  virtual void FindShortSuccessor(std::string* key) const override {
    BytewiseComparator()->FindShortSuccessor(key);
  }

 public:
  mutable size_t num_comparisons = 0U;
};

DB* PrepareDB(const Comparator* comparator) {
  Options options;
  // Disable RocksDB background compaction.
  DB* db = nullptr;
  ROCKSDB_NAMESPACE::DestroyDB(kDBPath, options);
  options.create_if_missing = true;
  options.write_buffer_size = 1 << 20;
  options.disable_auto_compactions = true;
  options.level0_slowdown_writes_trigger = 2000;
  options.level0_stop_writes_trigger = 3000;
  // options.level0_file_num_compaction_trigger = -1;
  options.compression = ROCKSDB_NAMESPACE::CompressionType::kNoCompression;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.stats_dump_period_sec = 30;
  options.comparator = comparator;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  assert(db);

  return db;
}

uint64_t num_inserted = 0U;
void InsertEntries(DB* db, const WriteOptions& write_options, int n_entries) {
  std::string value("", 1024);

  for(int k = 0; k < n_entries; k++) {
    // Leave priority 0 for the DeleteRange (smallest).
    uint8_t priority = 1 + rand() % kNPriorities;
    std::string key((char *)&priority, 1);
    uint32_t ikey = htonl(debug_info[priority].last_written);
    key += std::string((char *)&ikey, 4);
    db->Put(write_options, key, value);
    debug_info[priority].last_written++;
  }
}

void ValidateReadValue(const char *key) {  
  int p = key[0];
  uint32_t val = ntohl(*((uint32_t *)&key[1]));
  if (val != debug_info[p].last_read) {
    std::cout << "OOPS:" << val << ", " << debug_info[p].last_read << '\n';
    assert (val == debug_info[p].last_read);
  }
  assert(NoValBefore(p));
}

void PopAndVerifySeek(DB* db, const WriteOptions& write_options) {
  // now pop and verify
  int n_entries = rand() % 10 + 1;
  for(int k = 0; k < n_entries; k++) {
    std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    if (iter->Valid()) {
      const char *smallest_key = iter->key().data();
      ValidateReadValue(smallest_key);

      int p = smallest_key[0];
      debug_info[p].last_read++;
      db->Delete(write_options, iter->key());
      assert(NoValBefore(p));

      // ++num_popped;
      // if (num_popped % 100) {
      //   std::string min_key((char *)&p, 1);
      //   uint32_t ikey = htonl(0);
      //   min_key += std::string((char *)&ikey, 4);

      //   // std::cout << "Delete Range (p=" << p << '\n';
      //   auto s = db->DeleteRange(write_options, db->DefaultColumnFamily(), min_key, smallest_key);
      //   assert(s.ok());
      // }
    } else {
      assert(NoValBefore(1+ kNPriorities));
      break;
    }
  }
}

size_t get_smallest_count = 0U;
void PopAndVerifyGetSmallest(DB* db, const WriteOptions& write_options) {
  // now pop and verify
  int n_entries = rand() % 10 + 1;
  for(int k = 0; k < n_entries; k++) {
    std::string smallest_key;

    // ++get_smallest_count;
    // if (get_smallest_count == 882) {
    //   std::cout << "GetSmallest (#" << get_smallest_count << ")\n";
    //   gs_debug_prints = true;
    // }
    auto s = db->GetSmallest(ReadOptions(), db->DefaultColumnFamily(), &smallest_key, nullptr);
    // std::cout << "After GetSmallest (#" << get_smallest_count << ")\n";

    if (s.ok()) {
      const char *key = smallest_key.data();
      ValidateReadValue(key);

      int p = key[0];
      debug_info[p].last_read++;
      db->Delete(write_options, smallest_key);
      assert(NoValBefore(p));

      // ++num_popped;
      // if (num_popped % 100) {
      //   std::string min_key((char *)&p, 1);
      //   uint32_t ikey = htonl(0);
      //   min_key += std::string((char *)&ikey, 4);

      //   // std::cout << "Delete Range (p=" << p << '\n';
      //   s = db->DeleteRange(write_options, db->DefaultColumnFamily(), min_key, smallest_key);
      //   assert(s.ok());
      // }
    } else {
      assert(NoValBefore(1 + kNPriorities));
      break;
    }
  }
}

int main() {   
  using nano = std::chrono::nanoseconds;

  auto comparator = std::make_unique<CountingComparator>();
  auto db = PrepareDB(comparator.get());

  std::string value("", 1024);
  WriteOptions write_options;
  write_options.disableWAL = true;

#if USE_GET_SMALLEST  
  std::cout << "Starting - Get Smallest\n";
#else
  std::cout << "Starting - Seek\n";
#endif

  auto num_1000s_iters = 100;
  size_t total_iters = num_1000s_iters * 1000;
  auto start_time = std::chrono::high_resolution_clock::now();
  for (size_t i = 0U; i < total_iters; ++i) {
    // insert up to 10 random entries
    int n_entries = rand() % 10 + 1;
    InsertEntries(db, write_options, n_entries);

#if USE_GET_SMALLEST  
    PopAndVerifyGetSmallest(db, write_options);
#else
    PopAndVerifySeek(db, write_options);
#endif

    if (((i != 0) && ((i % 1000) == 999)) || (i == total_iters - 1)) {
      auto end_time = std::chrono::high_resolution_clock::now();
      nano total_time = end_time - start_time;
      std::cout << "Completed 1000 Iters (i = " << (i+1) << ") in " << std::fixed << std::setprecision(4) << (total_time.count() * 1e-9) << " seconds. #Comparisons:" << comparator->num_comparisons << std::endl;

      start_time = std::chrono::high_resolution_clock::now();
    }
  }

  delete db; 
}
