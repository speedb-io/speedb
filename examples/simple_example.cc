// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// An example code demonstrating how to use CompactFiles, EventListener,
// and GetColumnFamilyMetaData APIs to implement custom compaction algorithm.

#include <mutex>
#include <string>
#include <iostream>
#include <arpa/inet.h>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"

using ROCKSDB_NAMESPACE::ColumnFamilyMetaData;
using ROCKSDB_NAMESPACE::CompactionOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::EventListener;
using ROCKSDB_NAMESPACE::FlushJobInfo;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::FlushOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_delete_range_example";
#else
std::string kDBPath = "/tmp/rocksdb_delete_range_example";
#endif

extern bool gs_debug_prints;
bool gs_debug_prints = false;

namespace {
constexpr uint32_t PRIME = 999983; 

DB* RecreateDB() {
  Options options;  
  ROCKSDB_NAMESPACE::DestroyDB(kDBPath, options);

  options.compression = ROCKSDB_NAMESPACE::CompressionType::kNoCompression;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  options.disable_auto_compactions = true;
  options.max_write_buffer_number = 1000;
  options.level0_slowdown_writes_trigger = 2000;
  options.level0_stop_writes_trigger = 3000;
  options.level0_file_num_compaction_trigger = -1;

  DB* db = nullptr;
  auto s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  assert(db != nullptr);

  return db;
}

size_t PrepareDB(DB* db) {
  std::cout << "Preparing DB\n";

  WriteOptions write_options;
  write_options.disableWAL = true;

  std::string value("", 1024);

  auto clock = db->GetEnv()->GetSystemClock();

  std::cout << "Inserting keys\n";
  auto start_time = clock->NowMicros();
  for (uint32_t i = 0; i < PRIME ; ++i) {
    // make sure the keys are sorted
    size_t key = htonl((i * PRIME/100) % PRIME);
    auto s = db->Put(write_options, std::string(reinterpret_cast<char*>(&key), 8), value);
    assert(s.ok());
  }
  auto end_time = clock->NowMicros();
  std::cout << "Insert Completed in " << (end_time - start_time) << " micros\n";

  // now the delete (10% we delete using a normal delete)
  std::cout << "Deleting Single Keys\n";
  start_time = clock->NowMicros();
  for (uint32_t i = 0; i < PRIME/10 ; ++i) {
    size_t key = htonl(i);    
    auto s =db->Delete(write_options, std::string(reinterpret_cast<char*>(&key), 8));
    assert(s.ok());
  }
  auto end_time = clock->NowMicros();
  std::cout << "Deleting Single Keys Completed in " << (end_time - start_time) << " micros\n";

  std::cout << "Deleting A Range\n";
  size_t start_key = htonl(PRIME/10 + 100);
  size_t end_key = htonl(PRIME/10  + 100 + PRIME/10);
  auto s = db->DeleteRange( write_options, db->DefaultColumnFamily(),
                            std::string(reinterpret_cast<char*>(&start_key), 8),
                            std::string(reinterpret_cast<char*>(&end_key), 8));
  assert(s.ok());

  return start_key;
}

}

void RunGetSmallestTest() {
  auto db = RecreateDB();
  size_t start_key = PrepareDB(db);

  auto clock = db->GetEnv()->GetSystemClock();

  std::cout << "Seeking using a DB Iterator\n";
  auto start_time = clock->NowMicros();
  size_t key = htonl(0);
  for (uint32_t i = 0; i < 1000000 ; i++) {
    auto iter = db->NewIterator(ReadOptions());
    iter->Seek(std::string(reinterpret_cast<char*>(&key), 8));
    delete iter;
  }
  std::cout << "The time to get 1000  keys after delete is %lu micros\n",   clock->NowMicros() - t);
  exit(0);
  t = clock->NowMicros();

  for (uint32_t i = 0; i < 1000 ; i++) {
    auto iter = db->NewIterator(ReadOptions());
    iter->Seek(std::string(reinterpret_cast<char*>(&start_key), 8));
    delete iter;
  }
  printf("time to get 1000  keys after range delete is %lu micros\n",   clock->NowMicros() -t);
  t = clock->NowMicros();
  auto delete_range_start = + PRIME/10  + 100 + PRIME/10;
  for (uint32_t i = 0; i < 1000 ; i++) {
    auto iter = db->NewIterator(ReadOptions());    
    start_key = htonl(rand() % (PRIME - delete_range_start) + delete_range_start); 
    iter->Seek(std::string(reinterpret_cast<char*>(&start_key), 8));
    delete iter;
  }
  printf("time to get 1000  keys with no delete is %lu micros\n",   clock->NowMicros() -t);
  delete db;
}

int main() {
  RunGetSmallestTest();
}