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

#include <gflags/gflags.h>
#include <functional>
#ifndef GFLAGS_NAMESPACE
// in case it's not defined in old versions, that's probably because it was
// still google by default.
#define GFLAGS_NAMESPACE google
#endif

#include <unistd.h>
#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"

using namespace ROCKSDB_NAMESPACE;
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;
using GFLAGS_NAMESPACE::SetVersionString;

DEFINE_string(db_path, "/tmp/secondary_test", "path to the database");
DEFINE_string(secondary_path, "", "secondary path, if not specified use {db_path}_{pid}");
DEFINE_bool(writer, false, "run writer process");
DEFINE_bool(reader, false, "run reader process");
  
DEFINE_int64(write_size_bytes, 1024, "size of each write");
DEFINE_int64(batch_size_bytes, 20*1024*1024, "size of each batch");
DEFINE_bool(manual_flush, false, "perform manual flush at the end of each batch write");
DEFINE_bool(disable_wal, false, "write without a WAL, manual flush is recommended in such case");
DEFINE_int64(cycle_duration, 12, "write batch/catchup cycle duration in seconds");
DEFINE_int64(run_duration, 600, "run duration in seconds");
DEFINE_int64(write_buffer_size, 1024*1024*64, "write buffer size, if not specified the default is 64MB");

void do_writes() {
  DB *db;
  Options options;
  // t
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  options.stats_dump_period_sec = 30;
  options.write_buffer_size = FLAGS_write_buffer_size;

  // open DB
  Status s = DB::Open(options, FLAGS_db_path, &db);
  assert(s.ok());
  int writes_per_cycle = FLAGS_batch_size_bytes / FLAGS_write_size_bytes;
  auto env = db->GetEnv();
  auto *clock = env->GetSystemClock().get();
  auto start = clock->NowMicros();
  auto finish = start + 1000000 * FLAGS_run_duration;
  auto cycle_time_in_micros = FLAGS_cycle_duration * 1000000;
  
  std::string value("", FLAGS_write_size_bytes);
  WriteOptions write_options;
  write_options.disableWAL = FLAGS_disable_wal;

  while (start < finish) {
    for (int i = 0; i < writes_per_cycle; i++) {
      int key = rand();
      s = db->Put(write_options, Slice((char *)&key, sizeof(key)).ToString(),
                  value);
      assert(s.ok());
    }
    if (FLAGS_manual_flush) {
      db->Flush(FlushOptions());
    }

    auto write_duration = clock->NowMicros() - start;
    if (write_duration > cycle_time_in_micros) {
      std::cerr << "Write cycle took too long: " << write_duration << "us\n";
    } else {
      std::cout << "Write cycle duration: " << write_duration << "us\n";

      clock->SleepForMicroseconds( cycle_time_in_micros - write_duration );
    }
    start = clock->NowMicros();
  }

  delete db;
}

void do_reads() {
  DB* db;
  Options options;
  // create the DB if it's not already present
  options.create_if_missing = false;
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  options.stats_dump_period_sec = 30;

  std::string secondary_path = FLAGS_secondary_path;
  if (secondary_path.empty()) {
    int r = rand();
    secondary_path = FLAGS_db_path + std::string("_") + std::to_string(getpid());
  }  
  Status s = DB::OpenAsSecondary(options, FLAGS_db_path, secondary_path, &db);
  assert(s.ok());
  auto env = db->GetEnv();
  auto *clock = env->GetSystemClock().get();
  auto start = clock->NowMicros();
  auto finish = start + 1000000 * FLAGS_run_duration;
  auto cycle_time_in_micros = FLAGS_cycle_duration * 1000000;
  
  std::string value("", FLAGS_write_size_bytes);

  while (start < finish) {
    db->TryCatchUpWithPrimary();    
    auto catchup_duration = clock->NowMicros() - start;

    if (catchup_duration > cycle_time_in_micros) {
      std::cerr << "catch up took too long: " << catchup_duration << "us\n";
    } else {
      std::cout << "catch up duration: " << catchup_duration << "us\n";

      clock->SleepForMicroseconds( cycle_time_in_micros - catchup_duration );
    }
    start = clock->NowMicros();
  }

  delete db;
} 
   


int main(int argc, char **argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  srand(time(0));
  ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_writer == FLAGS_reader) {
	  std::cerr << "Either reader or writer should be specified\n";
	  return -1;
  }

  if (FLAGS_writer) {
    do_writes();
  } else {
    do_reads();
  }
    
    
}
