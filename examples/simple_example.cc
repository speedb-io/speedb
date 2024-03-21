// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include<iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/iterator.h"
namespace fs = std::filesystem;

using namespace ROCKSDB_NAMESPACE;
using namespace std;
#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

DB* db;
std::string sst_root_dir = "/tmp/test/";
vector<string>  paths;
// #define MAX_KEYS 100000

int max_keys = 0, per_iter_keys = 0, conc_writes = 0;

int create_sst_file(int start, int end);
void ingest_file();

bool create_sst_files() {
  for(int i=1;i<max_keys;i+=per_iter_keys) {
    bool res = create_sst_file(i, i+per_iter_keys);
    if(!res) {
      return false;
    }
  }
  return true;
}

vector<string> get_all_files(string dir_path) {
  return paths;
}

int create_sst_file(int start, int end) {
  Options options;

  SstFileWriter sst_file_writer(EnvOptions(), options);
  string file_path = sst_root_dir + std::to_string(start);
  paths.push_back(file_path);
  Status s = sst_file_writer.Open(file_path);
  if (!s.ok()) {
      printf("Error while opening file %s, Error: %s\n", file_path.c_str(),
            s.ToString().c_str());
      return false;
  }

  // Insert rows into the SST file, note that inserted keys must be 
  // strictly increasing (based on options.comparator)
  std::vector<std::string> nums;
  for(int i=start;i<=end;i++) {
    if(i%10 == 0) continue;
    nums.push_back(std::to_string(i));
  }
  std::sort(nums.begin(), nums.end());
  for(auto num : nums) {
    s = sst_file_writer.Put(num, num);
    if (!s.ok()) {
            printf("Error while adding Key: %s, Error: %s\n", num.c_str(),
            s.ToString().c_str());
      return false;
    }
  }

  // Close the file
  s = sst_file_writer.Finish();
  if (!s.ok()) {
      printf("Error while finishing file %s, Error: %s\n", file_path.c_str(),
            s.ToString().c_str());
      return false;
  }
  return true;
}

void ingest_file() {
    cout<<"executing ingest_file\n";
    IngestExternalFileOptions ifo;
    vector<string> ssts_files = get_all_files(sst_root_dir);
    Status s = db->IngestExternalFile(ssts_files, ifo);
    if (!s.ok()) {
      printf("Error while ingesting files. Error %s\n", s.ToString().c_str());
      return;
    }
    return;
}

void read_kvs() {
  auto it = db->NewIterator(ReadOptions()); 
  cout<<"Executing  read_kvs\n";
  int iter = 100;
  while(iter>0) {
    
    
    it->SeekToFirst();
    while(it->Valid()) {
      string key = it->key().ToString();
      string val = it->value().ToString();
      cout<<"Key: "<<key<<" value: "<<val<<endl; 
      it->Next();
    }
    iter--;
  }
}

void write_kvs() {
  cout<<"Executing  write_kvs\n";
  // Put key-value
  for(int i=1; i<conc_writes;i++) {
    string kv = to_string(i);
    Status s = db->Put(WriteOptions(), kv, kv);
  }
}


int main(int argc, char *argv[]) {

  //create one instance of db for both ingesting and directly writing new keys to the db. db is global variable
  Options options;
  conc_writes = (int) std::stoi(argv[1]);
  max_keys = (int) std::stoi(argv[2]);
  per_iter_keys = (int) std::stoi(argv[3]);
  options.IncreaseParallelism();
  // options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;
  cout<<"Default Memtable size: "<<options.write_buffer_size<<endl;
  options.write_buffer_size = 1024;
  Status s = DB::Open(options, kDBPath, &db);
  
  if(!s.ok()) {
    cout<<"Error while opening database: "<<s.ToString().c_str()<<endl;
  }
  assert(s.ok());

  //create sst
  create_sst_files();
  std::cout<<"SST created"<<std::endl;

  //ingest and write in parallel to reproduce.
  std::thread t2(ingest_file); 
  std::thread t1(write_kvs); 
  std::thread t3(read_kvs);
  t1.join();
  t2.join();
  t3.join();
  std::cout<<"Ingestion completed"<<std::endl;
  return 0;
}
  