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
  std::vector<ColumnFamilyHandle*> handles(2);
  DestroyDB(db_path, options);
  auto s = DB::Open(options, db_path,  &db);
  assert(s.ok());

  for (int i = 0; i < 2; i++) {
    char cf_name[64];
    sprintf(cf_name, "cf_%d", i);
    s = db->CreateColumnFamily(ColumnFamilyOptions(), cf_name, &handles[i]);
    assert(s.ok());
  }
  
  const int kDataSize = 1024;
  int val[kDataSize / sizeof(int)];
  for (int i = 0; i < kDataSize / sizeof(int); i++) val[i] = rand();
  const uint32_t max_table_key = 10 * 1000 * 1000;
  RateLimiter *write_rate_limiter = nullptr;
  if (write_rate) {
    write_rate_limiter = NewGenericRateLimiter(write_rate * 1024);
  }
  WriteOptions wr_options;
  wr_options.disableWAL = true;
  size_t start_time = time(0);
  for (size_t i = 0; i < max_table_key * 10ul; i++) {
    uint32_t key = htonl(rand() % max_table_key);
    if (write_rate_limiter) {
      write_rate_limiter->Request(kDataSize, Env::IO_HIGH, nullptr /* stats */,
                                  RateLimiter::OpType::kWrite);
    }
    const size_t rand_f = 4096;
    int handle_id = (rand() % rand_f) == 0 ? 0:1;
    for (int j = 0; j <= tid; j++) {
      s = db->Put(wr_options, handles[handle_id],
                  std::string((char *)&key, sizeof(uint32_t)),
                  std::string((char *)val, 1024));
      ios[tid]++;
    }
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
  std::thread *threads[10];
  std::thread *reporter;
  for (int i = 0; i < 1; i++) {
    threads[i] = new std::thread(write_thread, i, i*10*1024);
    ios[i] = 0;
  }
  reporter = new std::thread(report_thread, 1);
  reporter->join();
  return 0;
}
