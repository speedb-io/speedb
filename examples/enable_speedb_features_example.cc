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
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::SharedOptions;
using ROCKSDB_NAMESPACE::SharedOptionsSpeeDB;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\enable_speedb_features_example";
#else
std::string kDBPath1 = "/tmp/enable_speedb_features_example1";
std::string kDBPath2 = "/tmp/enable_speedb_features_example2";
#endif

int main() {
  DB *db1, *db2;
  Options op1, op2;
  size_t total_ram_size_bytes = 512 * 1024 * 1024;
  size_t delayed_write_rate = 256 * 1024 * 1024;
  int total_threads = 8;
  std::shared_ptr<SharedOptionsSpeeDB> so =
      std::shared_ptr<SharedOptionsSpeeDB>(new SharedOptionsSpeeDB(
          total_ram_size_bytes, total_threads, delayed_write_rate));
  // create the DB if it's not already present
  op1.create_if_missing = true;
  op2.create_if_missing = true;
  op1.EnableSpeedbFeatures(&so);
  op2.EnableSpeedbFeatures(&so);

  std::cout << "0" << std::endl;
  // open DB
  Status s = DB::Open(op1, kDBPath1, &db1);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }

  std::cout << "1" << std::endl;
  s = DB::Open(op2, kDBPath2, &db2);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  std::cout << "2" << std::endl;

  // Put key-value
  s = db1->Put(WriteOptions(), "key1", "value");
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  std::string value;
  // get value
  s = db1->Get(ReadOptions(), "key1", &value);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  std::cout << "3" << std::endl;

  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db1->Write(WriteOptions(), &batch);
  }
  std::cout << "4" << std::endl;

  s = db1->Get(ReadOptions(), "key1", &value);
  if (!s.IsNotFound()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  db1->Get(ReadOptions(), "key2", &value);
  assert(value == "value");
  std::cout << "5" << std::endl;

  {
    PinnableSlice pinnable_val;
    s = db1->Get(ReadOptions(), db1->DefaultColumnFamily(), "key2",
                 &pinnable_val);
    if (strcmp(pinnable_val.data(), "value") != 0) {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
  }
  std::cout << "6" << std::endl;

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    s = db1->Get(ReadOptions(), db1->DefaultColumnFamily(), "key2",
                 &pinnable_val);
    if (strcmp(pinnable_val.data(), "value") != 0) {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
    // If the value is not pinned, the internal buffer must have the value.
    if (!pinnable_val.IsPinned() && string_val != "value") {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
  }
  std::cout << "7" << std::endl;

  {
    PinnableSlice pinnable_val;
    s = db1->Get(ReadOptions(), db1->DefaultColumnFamily(), "key1",
                 &pinnable_val);
    if (!s.IsNotFound()) {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
    std::cout << "8" << std::endl;

    // Reset PinnableSlice after each use and before each reuse
    pinnable_val.Reset();
    db1->Get(ReadOptions(), db1->DefaultColumnFamily(), "key2", &pinnable_val);
    if (pinnable_val != "value") {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
    std::cout << "9" << std::endl;

    pinnable_val.Reset();
    // The Slice pointed by pinnable_val is not valid after this point
  }

  // Put key-value
  s = db2->Put(WriteOptions(), "key1", "value");
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  // get value
  s = db2->Get(ReadOptions(), "key1", &value);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  std::cout << "10" << std::endl;

  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db2->Write(WriteOptions(), &batch);
  }
  std::cout << "11" << std::endl;

  s = db2->Get(ReadOptions(), "key1", &value);
  if (!s.IsNotFound()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  db2->Get(ReadOptions(), "key2", &value);
  assert(value == "value");
  std::cout << "12" << std::endl;

  {
    PinnableSlice pinnable_val;
    s = db2->Get(ReadOptions(), db2->DefaultColumnFamily(), "key2",
                 &pinnable_val);
    if (strcmp(pinnable_val.data(), "value") != 0) {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
  }
  std::cout << "13" << std::endl;

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    s = db2->Get(ReadOptions(), db2->DefaultColumnFamily(), "key2",
                 &pinnable_val);
    if (strcmp(pinnable_val.data(), "value") != 0) {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
    // If the value is not pinned, the internal buffer must have the value.
    if (!pinnable_val.IsPinned() && string_val != "value") {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
  }
  std::cout << "14" << std::endl;

  {
    PinnableSlice pinnable_val;
    s = db2->Get(ReadOptions(), db2->DefaultColumnFamily(), "key1",
                 &pinnable_val);
    if (!s.IsNotFound()) {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
    std::cout << "15" << std::endl;

    // Reset PinnableSlice after each use and before each reuse
    pinnable_val.Reset();
    db2->Get(ReadOptions(), db2->DefaultColumnFamily(), "key2", &pinnable_val);
    if (pinnable_val != "value") {
      std::cout << s.ToString() << std::endl;
      return 1;
    }
    std::cout << "16" << std::endl;

    pinnable_val.Reset();
    // The Slice pointed by pinnable_val is not valid after this point
  }

  delete db1;
  delete db2;

  return 0;
}
