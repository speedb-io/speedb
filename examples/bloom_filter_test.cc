// Copyright (c) 2011-present, Speedb, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <unistd.h>


#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include <rocksdb/filter_policy.h>
#include <rocksdb/configurable.h>
#include <rocksdb/convenience.h>
#include <rocksdb/filter_policy.h>

#include <arpa/inet.h>


using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::ConfigOptions;
using ROCKSDB_NAMESPACE::FilterPolicy;



#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_bloom_filter";
#else
std::string kDBPath = "/data/db/bloom_filter";
#endif
static size_t scramble(size_t inp)
{
  return (inp << 32) | (inp * 11111) | inp;

}

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.compression = rocksdb::kNoCompression;
  auto table_options =
    options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
#if SPDB_BLOOM
  {
    ConfigOptions config_options;
    config_options.ignore_unsupported_options = false;
    Status s = FilterPolicy::CreateFromString(config_options, std::string("spdb.PairedBloomFilter:16"),
                                              &table_options->filter_policy);
    assert(s.ok());
  }
#else
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(16));
#endif

  // create the DB if it's not already present
  options.create_if_missing = true;
  for (size_t i = 0; i < options.compression_per_level.size(); i++) {
    options.compression_per_level[i] = rocksdb::kNoCompression;
  }

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  const int kDataSize = 1024;
  int val[kDataSize/sizeof(int)];
  for (int i =0; i < kDataSize/sizeof(int); i++)
    val[i] = rand();
  const size_t max_table1_key = 100 * 1000 * 1000;
  const size_t max_table2_key = 100 * 1000 * 1000;

  time_t t = time(0);
  WriteOptions write_options;
  ReadOptions read_options;
  write_options.disableWAL = true;

  // do some sequntial writes
  for (uint32_t i = 0; i < max_table1_key; i++) {
    size_t key = scramble(i);
    s = db->Put(write_options, std::string((char *)&key, sizeof(key)),
                std::string((char *)val, 1024));
  }

  printf(" ramdom insert took %lu seconds \n", time(0) - t);
  // wait for the noise to end
  sleep(600);
  // now start read before write test
  t=time(0);
  size_t n_found = 0;
  for (int i = 0; i < max_table2_key ; i++) {
    size_t key = scramble(i+max_table1_key);
    std::string get_val;
    s = db->Get(read_options, std::string((char *)&key, sizeof(key)), &get_val);
    if (!s.ok()) {
      s = db->Put(write_options, std::string((char *)&key, sizeof(key)),
                  std::string((char *)val, 1024));     
    } else {
      n_found++;
    }
  }
  printf(" read befor write took  %lu seconds %lu (%lu) \n", time(0) - t, n_found, max_table2_key);
  t = time(0);

  delete db;

  return 0;
}
