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

int main() {
  Options options;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  DB* db = nullptr;
  ROCKSDB_NAMESPACE::DestroyDB(kDBPath, options);
  options.compression = ROCKSDB_NAMESPACE::CompressionType::kNoCompression;
  
 
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  assert(db);
  const uint32_t prime= 999983; 
  std::string value("", 1024);
  WriteOptions write_options;
  write_options.disableWAL = true;
  auto clock = db->GetEnv()->GetSystemClock();
  auto t = clock->NowMicros();
  size_t time2 = 0;
  for (uint32_t i = 0; i < prime ; ++i) {
    // make sure the keys are sorted
    size_t key = htonl((i * prime/100) % prime);
    db->Put(write_options, std::string((char *)&key, 8), value);
  }
  printf ("insert completed %lu micros\n", clock->NowMicros() - t);
  t = clock->NowMicros();
  // disable the auto compaction
  options.disable_auto_compactions=true;

  // now the delete (10% we delete using a normal delete)
  for (uint32_t i = 0; i < prime/10 ; ++i) {
    size_t key = htonl(i);    
    db->Delete(write_options, std::string((char *)&key, 8));
  }

  size_t start_key = htonl(prime/10 + 100);
  size_t end_key = htonl(prime/10  + 100 + prime/10);
  db->DeleteRange(write_options, db->DefaultColumnFamily(),
                  std::string((char *)&start_key, 8),
                  std::string((char *)&end_key, 8));
  
  printf ("delete completed %lu micros\n", clock->NowMicros() -t );
  t = clock->NowMicros();
  size_t key = htonl(0);
  for (uint32_t i = 0; i < 1000000 ; i++) {
    auto iter = db->NewIterator(ReadOptions());
    iter->Seek(std::string((char *)&key, 8));
    delete iter;
  }
  printf("time to get 1000  keys after delete is %lu micros\n",   clock->NowMicros() - t);
  exit(0);
  t = clock->NowMicros();

  for (uint32_t i = 0; i < 1000 ; i++) {
    auto iter = db->NewIterator(ReadOptions());
    iter->Seek(std::string((char *)&start_key, 8));
    delete iter;
  }
  printf("time to get 1000  keys after range delete is %lu micros\n",   clock->NowMicros() -t);
  t = clock->NowMicros();
  auto delete_range_start = + prime/10  + 100 + prime/10;
  for (uint32_t i = 0; i < 1000 ; i++) {
    auto iter = db->NewIterator(ReadOptions());    
    start_key = htonl(rand() % (prime - delete_range_start) + delete_range_start); 
    iter->Seek(std::string((char *)&start_key, 8));
    delete iter;
  }
  printf("time to get 1000  keys with no delete is %lu micros\n",   clock->NowMicros() -t);
    
  

  delete db;

  return 0;
}