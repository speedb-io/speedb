// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// An example code demonstrating how to use CompactFiles, EventListener,
// and GetColumnFamilyMetaData APIs to implement custom compaction algorithm.

#include <mutex>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

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
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_compact_files_example";
#else
std::string kDBPath = "/tmp/rocksdb_compact_files_example";
#endif


int main() {
  Options options;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  options.disable_auto_compactions = true;
  DB* db = nullptr;
  ROCKSDB_NAMESPACE::DestroyDB(kDBPath, options);
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  assert(db);

  // if background compaction is not working, write will stall
  // because of options.level0_stop_writes_trigger
  
  for (int i = 1; i < 1999; ++i) {
    db->Put(WriteOptions(), std::to_string(i), std::to_string(i));
  }
  db->Flush(FlushOptions());
  std::string value;
  for (int i = 1; i < 1999; ++i) {
    db->Put(WriteOptions(), std::to_string(i), std::to_string(i));
  }
  
  for (int i = 54; i <= 66; ++i) {
    auto s=db->Get(ReadOptions(), std::to_string(i), &value);
    if (!s.IsNotFound()) {
      printf("found value for %d\n",i);
    }
  }
  printf ("delete range [54, 66) \n");
  db->DeleteRange(WriteOptions(), nullptr, std::to_string(54), std::to_string(66));
  // verify the values are not there
  for (int i = 54; i <= 66; ++i) {
    auto s=db->Get(ReadOptions(), std::to_string(i), &value);
    if (!s.IsNotFound()) {
      printf("found value for %d\n",i);
    }
  }
  printf ("flush \n");
  db->Flush(FlushOptions());
  for (int i = 54; i <= 66; ++i) {
    auto s=db->Get(ReadOptions(), std::to_string(i), &value);
    if (!!s.IsNotFound()) {
      printf("found value for %d\n",i);
    }
  }
  delete db;

  return 0;
}
