// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <cstdio>
#include <string>
#include <vector>
#include <thread>
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_column_families_example";
#else
std::string kDBPath = "/tmp/rocksdb_column_families_example";
#endif

using namespace ROCKSDB_NAMESPACE;


std::shared_ptr<WriteBufferManager> wbm;
#include <arpa/inet.h>
static int ios[10];


static int write_thread(int tid, size_t write_rate) {

  printf("starting thread %d write rate %luKB/s \n", tid, write_rate);
  DB* db;
  Options options;  
  options.create_if_missing = true;
  options.IncreaseParallelism();
  options.compression = rocksdb::kNoCompression;
  options.write_buffer_size = 1024 * 1024 * 256;
  options.write_buffer_manager = wbm;

  // open DB with N column families  
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(ColumnFamilyDescriptor(
      ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, ColumnFamilyOptions()));

  char tsuf[64];
  sprintf(tsuf, "%d", tid);
  std::string db_path = kDBPath + tsuf;
  DestroyDB(db_path, options);
  auto s = DB::Open(options, db_path,  &db);
  assert(s.ok());

  WriteOptions wr_options;
  wr_options.disableWAL = true;
  size_t start_time = time(0);
  // allocate 200MB
  for (size_t i = 0; i < 200*1024; i++) {
    uint32_t key = htonl(rand);
    s = db->Put(wr_options, 
                std::string((char *)&key, sizeof(uint32_t)),
                std::string((char *)val, 1024));
    ios[tid]++;
  }
  char tsuf_2[64];
  sprintf(tsuf_2, "%d-2", tid);
  db_path = kDBPath + tsuf_2;
  DestroyDB(db_path, options);
  auto s = DB::Open(options, db_path,  &db);
  assert(s.ok());
  for (size_t i = 0; i < 1024*1024*1024; i++) {
    uint32_t key = htonl(rand);
    s = db->Put(wr_options, 
                std::string((char *)&key, sizeof(uint32_t)),
                std::string((char *)val, 1024));
    ios[tid]++;
  }
  
  sleep(1800);
  delete db;
  return 0;
}


void report_thread(int count)
{
  for (int t = 0; t < 1800; t++)  {
    sleep(1);
    printf("%d ,", t);
    for (int i = 0; i < count; i++) {
      printf("%d , ", ios[i]);
    }
    printf("\n" );
  } 
}

int main() {
  
  wbm.reset(new  WriteBufferManager(1024 * 1024 * 256, {}, true));
  std::thread *reporter;
  for (int i = 0; i < 1; i++) {
    threads[i] = new std::thread(write_thread, i, 0);
    ios[i] = 0;
  }  
  reporter = new std::thread(report_thread, 1);
 
  reporter->join();
  return 0;
}
