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

#include <unistd.h>

#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/db_ttl.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\speedb_with_ttl_example";
#else
std::string kDBPath = "/tmp/speedb_with_ttl_example";
#endif

int main() {
  // Open the storage
  DBWithTTL* db = nullptr;
  Options options;
  // Create the DB if it's not already present
  options.create_if_missing = true;
  // Configure time to live of the objects
  int32_t ttl = 1;
  // Keys to insert to the db
  std::string key1 = "key_1";
  std::string key2 = "key_2";
  std::string key3 = "key_3";
  // Value for the keys
  std::string put_value1 = "1 Speedb is awesome!";
  std::string put_value2 = "2 Speedb is awesome!";
  std::string put_value3 = "3 Speedb is awesome!";
  // Value to fetch from the db
  std::string get_value;
  ReadOptions ropts = ReadOptions();
  // Configure that we will not get keys that have been expired by ttl.
  // The default behaviour is to return keys until the compation will delete.
  ropts.skip_expired_data = true;
  std::vector<rocksdb::Slice> keys = {key1, key2};
  std::vector<std::string> values;

  Status s = DBWithTTL::Open(options, kDBPath, &db, ttl);
  assert(s.ok());

  s = db->Put(WriteOptions(), key1, put_value1);
  assert(s.ok());
  s = db->Put(WriteOptions(), key2, put_value2);
  assert(s.ok());
  s = db->Get(ropts, key1, &get_value);
  assert(s.ok());
  std::cout << "The value returned by db Get before expiration is: "
            << std::endl
            << get_value << std::endl
            << std::endl;
  std::cout << "The value returned by db MultiGet before expiration are: "
            << std::endl;
  auto statuses = db->MultiGet(ropts, keys, &values);
  for (const auto& status : statuses) {
    assert(status.ok());
  }
  for (const auto& value : values) {
    std::cout << value << std::endl;
  }
  std::cout << std::endl;
  // sleeps more than the ttl to emphasize the expiration of objects
  sleep(ttl + 1);

  s = db->Get(ropts, key1, &get_value);
  if (s.IsNotFound()) {
    std::cout << "Key has been expired as expected by Get" << std::endl;
  }
  statuses = db->MultiGet(ropts, keys, &values);
  for (const auto& i : statuses) {
    if (i.IsNotFound()) {
      std::cout << "Key has been expired as expected by MultiGet" << std::endl;
    }
  }
  ropts.skip_expired_data = false;
  std::cout << "Keys actually stored but expired by MultiGet, without "
               "skip_expired_data"
            << std::endl;
  statuses = db->MultiGet(ropts, keys, &values);
  for (size_t i = 0; i < statuses.size(); ++i) {
    if (statuses[i].ok()) {
      std::cout << keys[i].ToStringView() << ":" << values[i] << std::endl;
    }
  }
  ropts.skip_expired_data = true;
  db->SetTtl(1000);
  s = db->Get(ropts, key1, &get_value);
  assert(s.ok());
  // close DB
  s = db->Close();
  s = DBWithTTL::Open(options, kDBPath, &db, ttl, true);
  sleep(ttl + 1);
  s = db->Get(ropts, key1, &get_value);
  assert(s.IsNotFound());
  std::cout << "Open DB with read_only will not return expired keys "
            << std::endl
            << std::endl;
  db->Close();
  s = DBWithTTL::Open(options, kDBPath, &db, ttl);
  ropts = ReadOptions();
  ropts.skip_expired_data = true;
  s = db->Put(WriteOptions(), key3, put_value3);
  auto it = db->NewIterator(ropts);

  assert(s.ok());

  it->SeekToFirst();
  if (it->Valid()) {
    // Because key_1 and key_2 expired this line should print key_3
    std::cout << "skip to: " << it->key().ToStringView() << std::endl;
  }
  delete it;
  delete db;
  return 0;
}