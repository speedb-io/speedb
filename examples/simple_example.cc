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

#include <iostream> 
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/statistics.h"

extern bool gs_debug_prints;
bool gs_debug_prints = false;

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_delete_range_example";
#else
std::string kDBPath = "/tmp/rocksdb_delete_range_example";
#endif

struct PerPriority
{
  uint32_t last_read = 0;
  uint32_t last_written = 0;
  bool empty() {return last_read == last_written;}
};

static constexpr int kNPriorities=32;
PerPriority debug_info[kNPriorities];
bool NoValBefore(uint p) {
  for (uint i = 0; i < p; i++) {
    if (!debug_info[i].empty())
      return false;
  }
  return true;
}

DB* PrepareDB() {
  Options options;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  DB* db = nullptr;
  ROCKSDB_NAMESPACE::DestroyDB(kDBPath, options);
  options.compression = ROCKSDB_NAMESPACE::CompressionType::kNoCompression;
  options.statistics =   ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.stats_dump_period_sec=30;
  options.disable_auto_compactions = true; 
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  assert(db);

  return db;
}

void InsertEntries(DB* db, const WriteOptions& write_options, int n_entries) {
  std::string value("", 1024);

  for(int k = 0; k < n_entries; k++) {
    uint8_t priority = rand() % kNPriorities;
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

int main() { 
  auto db = PrepareDB();

  std::string value("", 1024);
  WriteOptions write_options;
  write_options.disableWAL = true;
  auto clock = db->GetEnv()->GetSystemClock();
  auto t = clock->NowMicros(); 
  std::cout << "Starting\n";
  for (size_t i = 0; i < 1000 * 10 ; ++i) {
    {
      // insert up to 10 random entries
      int n_entries = rand() % 10 + 1;
      InsertEntries(db, write_options, n_entries);
    }
    { // now pop and verify
      int n_entries = rand() % 10 + 1;
      for(int k = 0; k < n_entries; k++) {
        std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
        iter->SeekToFirst();
        if (iter->Valid()) {
          const char *key = iter->key().data();
          ValidateReadValue(key);

          int p = key[0];
          debug_info[p].last_read++;
          db->Delete(write_options, iter->key());
          assert(NoValBefore(p));
        } else {
          assert(NoValBefore(kNPriorities));
          break;
        }
      }
    }
  }

  delete db; 
}
