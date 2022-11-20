// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <iostream>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\check_artifacts";
#else
std::string kDBPath = "/tmp/check_artifacts";
#endif

int main() {
  DB* db;
  Options options;
  int counter;

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  ReadOptions ropts;
  ropts.verify_checksums = true;
  ropts.total_order_seek = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  Iterator* iter = db->NewIterator(ropts);
  // verify db is empty
  iter->SeekToFirst();
  if (iter->Valid()) {
    delete iter;
    delete db;
    db = nullptr;
    s = DB::Open(options, kDBPath, &db);
    assert(s.ok());
    iter = db->NewIterator(ropts);
  }

  // Put key-value
  s = db->Put(WriteOptions(), "1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("1");
    batch.Put("2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "2", &value);
  assert(value == "value");

  s = db->Put(WriteOptions(), "4", "value3");
  assert(s.ok());

  // Seek for key
  iter->SeekToFirst();
  iter->Seek("3");
  counter = 0;
  while (iter->Valid()) {
    iter->Next();
    counter++;
  }
  assert(counter == 1);

  // value is bigger than the max value in db
  iter->Seek("9");
  counter = 0;
  while (iter->Valid()) {
    iter->Next();
    counter++;
  }
  assert(counter == 0);

  // value is smaller than the min value in db
  iter->Seek("1");
  counter = 0;
  while (iter->Valid()) {
    iter->Next();
    counter++;
  }
  assert(counter == 2);

  // seek for the last
  iter->Seek("4");
  counter = 0;
  while (iter->Valid()) {
    iter->Next();
    counter++;
  }
  assert(counter == 1);

  {
    PinnableSlice pinnable_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "2", &pinnable_val);
    assert(pinnable_val == "value");
  }

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "2", &pinnable_val);
    assert(pinnable_val == "value");
    // If the value is not pinned, the internal buffer must have the value.
    assert(pinnable_val.IsPinned() || string_val == "value");
  }

  PinnableSlice pinnable_val;
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "1", &pinnable_val);
  assert(s.IsNotFound());
  // Reset PinnableSlice after each use and before each reuse
  pinnable_val.Reset();
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "2", &pinnable_val);
  assert(pinnable_val == "value");
  pinnable_val.Reset();
  // The Slice pointed by pinnable_val is not valid after this point
  delete iter;
  delete db;
  return 0;
}
