// Copyright (C) 2023 Speedb Ltd. All rights reserved.
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

#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\speedb_is_awesome_example";
#else
std::string kDBPath = "/tmp/speedb_is_awesome_example";
#endif

int main() {
  // Open the storage
  DB* db = nullptr;
  Options options;
  // create the DB if it's not already present
  options.create_if_missing = true;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // append new entry
  std::string key = "key_1";
  std::string put_value = "Speedb is awesome!";
  s = db->Put(WriteOptions(), key, put_value);
  assert(s.ok());

  // retrieve entry
  std::string get_value;
  s = db->Get(ReadOptions(), key, &get_value);
  assert(s.ok());
  assert(get_value == put_value);
  std::cout << get_value << std::endl;

  // close DB
  s = db->Close();
  assert(s.ok());
  return 0;
}
