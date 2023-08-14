// Copyright (C) 2022 Speedb Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdio>
#include <iostream>
#include <string>

#include "rocksdb/compression_type.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath1 = "C:\\Windows\\TEMP\\enable_speedb_features_example1";
std::string kDBPath2 = "C:\\Windows\\TEMP\\enable_speedb_features_example2";
std::string kDBPath3 = "C:\\Windows\\TEMP\\enable_speedb_features_example3";
std::string kDBPath4 = "C:\\Windows\\TEMP\\enable_speedb_features_example4";
#else
std::string kDBPath1 = "/tmp/enable_speedb_features_example1";
std::string kDBPath2 = "/tmp/enable_speedb_features_example2";
std::string kDBPath3 = "/tmp/enable_speedb_features_example3";
std::string kDBPath4 = "/tmp/enable_speedb_features_example4";
#endif

int main() {
  DB *db1 = nullptr;
  DB *db2 = nullptr;
  DB *db3 = nullptr;
  DB *db4 = nullptr;
  Options op1;
  Options op2;
  Options op3;
  Options op4;
  size_t total_ram_size_bytes = 512 * 1024 * 1024;
  size_t delayed_write_rate = 256 * 1024 * 1024;
  size_t total_threads = 8;

  // define SharedOptions object for each databases group
  SharedOptions so1(total_ram_size_bytes, total_threads, delayed_write_rate);

  // customize each options file except SpeedbSharedOptiopns members
  // as listed in the definition of SpeedbSharedOptiopns in options.h
  op1.create_if_missing = true;
  op1.compression = rocksdb::kNoCompression;
  //...
  op1.EnableSpeedbFeatures(so1);

  op2.create_if_missing = true;
  op2.compression = rocksdb::kZlibCompression;
  //...
  op2.EnableSpeedbFeatures(so1);

  // open the databases
  Status s = DB::Open(op1, kDBPath1, &db1);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }

  s = DB::Open(op2, kDBPath2, &db2);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }
  std::cout << "DBs group 1 was created" << std::endl;

  // do the same for any group of databases
  total_ram_size_bytes = 1024 * 1024 * 1024;
  delayed_write_rate = 128 * 1024 * 1024;
  total_threads = 4;
  SharedOptions so2(total_ram_size_bytes, total_threads, delayed_write_rate);

  // again customize each options object except SharedOptiopns members
  op3.create_if_missing = true;
  op3.compaction_style = rocksdb::kCompactionStyleUniversal;
  //...
  op3.EnableSpeedbFeatures(so2);

  op4.create_if_missing = true;
  op4.compaction_style = rocksdb::kCompactionStyleLevel;
  //...
  op4.EnableSpeedbFeatures(so2);

  // open the databases
  s = DB::Open(op3, kDBPath3, &db3);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }

  s = DB::Open(op4, kDBPath4, &db4);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }
  std::cout << "DBs group 2 was created" << std::endl;

  // creation of column family
  rocksdb::ColumnFamilyOptions cfo3(op3);
  rocksdb::ColumnFamilyHandle *cf;
  // coustomize it except SpeedbSharedOptiopns members

  // call EnableSpeedbFeaturesCF and supply for it the same SharedOptions
  // object as the DB, so2 this time.
  cfo3.EnableSpeedbFeaturesCF(so2);
  // create the cf
  s = db3->CreateColumnFamily(cfo3, "new_cf", &cf);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }
  std::cout << "new_cf was created in db3" << std::endl;

  s = db3->DropColumnFamily(cf);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }
  db3->DestroyColumnFamilyHandle(cf);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }
  std::cout << "new_cf was destroyed" << std::endl;

  s = db1->Close();
  assert(s.ok());
  s = db2->Close();
  assert(s.ok());
  s = db3->Close();
  assert(s.ok());
  s = db4->Close();
  assert(s.ok());

  delete db1;
  delete db2;
  delete db3;
  delete db4;

  return 0;
}
