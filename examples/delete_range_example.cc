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
using ROCKSDB_NAMESPACE::CompactRangeOptions;


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
  
  for (int i = 1; i < 100; ++i) {
    db->Put(WriteOptions(), std::to_string(i), std::to_string(i));
  }
  db->Flush(FlushOptions());
  // compact to level 3
  CompactRangeOptions cr_options;
  cr_options.change_level=true;
  cr_options.target_level=3;
  db->CompactRange(cr_options,nullptr, nullptr);
  for (int i = 1; i < 97; ++i) {
    db->Put(WriteOptions(), std::to_string(i), std::to_string(i));
  }
  db->Flush(FlushOptions());
  for (int i = 1; i < 97; ++i) {
    db->Put(WriteOptions(), std::to_string(i), std::to_string(i));
  }
  // this delete should delete the entrire db
  db->DeleteRange(WriteOptions(), nullptr, std::to_string(1), std::to_string(100));
  
  std::string value;
  s = db->Get(ReadOptions(), std::to_string(99), &value);
  if (s.ok()) {    
    printf("before flush found value!!\n");
  }
  db->Flush(FlushOptions());
  s = db->Get(ReadOptions(), std::to_string(99), &value);
  if (s.ok()) {    
    printf("after flush found value!!\n");
  }  
  cr_options.target_level=2;
  db->CompactRange(cr_options,nullptr, nullptr);
  s = db->Get(ReadOptions(), std::to_string(99), &value);
  if (s.ok()) {    
    printf("after compact range found value!!\n");
  }
  db->Close();
  delete db;

  return 0;
}
