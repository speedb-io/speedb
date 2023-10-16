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

#include <chrono>
#include <future>
#include <iostream>
#include <sstream>
#include <thread>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath =
    "C:\\Windows\\TEMP\\speedb_non_blocking_compact_range_example";
#else
std::string kDBPath = "/tmp/speedb_non_blocking_compact_range_example";
#endif

namespace {

// A Compaction Filter that is used to demonstrate the fact that a compaction
// was performed
class DestroyAllCompactionFilter : public CompactionFilter {
 public:
  DestroyAllCompactionFilter() {}

  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& existing_value,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return existing_value.ToString() == "destroy";
  }

  const char* Name() const override { return "DestroyAllCompactionFilter"; }
};

using CbFuture = std::future<Status>;

// The Non-Blocking manual compaction Callback Class
class CompactRangeCompleteCb : public CompactRangeCompletedCbIf {
 public:
  CompactRangeCompleteCb() {
    my_promise_ = std::make_unique<std::promise<Status>>();
  }

  CbFuture GetFuture() { return my_promise_->get_future(); }

  // This method will be called upon compact range completion
  void CompletedCb(Status completion_status) override {
    auto cb_tid = std::this_thread::get_id();
    std::cout
        << "[" << cb_tid
        << "] CompletedCb: Non-Blocking Compact Range Completed with status="
        << completion_status.ToString() << '\n';

    std::cout << "[" << cb_tid
              << "] CompletedCb: Sleeping in the callback for 2 seconds (Don't "
                 "do this in your code)\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Signal the completion and include the completion status
    std::cout << "[" << cb_tid << "] CompletedCb: Done Sleeping, Signal.\n";
    my_promise_->set_value(completion_status);
  }

 private:
  std::unique_ptr<std::promise<Status>> my_promise_;
};

}  // namespace

int main() {
  auto main_tid = std::this_thread::get_id();

  // Open the storage
  DB* db = nullptr;
  Options options;
  // Create the DB if it's not already present
  options.create_if_missing = true;
  options.compaction_filter = new DestroyAllCompactionFilter();
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  WriteOptions wo;

  // Inserting 4 keys to the DB, all have the value "destroy" except "key3"
  s = db->Put(wo, Slice("key1"), Slice("destroy"));
  assert(s.ok());
  s = db->Put(WriteOptions(), Slice("key2"), Slice("destroy"));
  assert(s.ok());
  s = db->Put(WriteOptions(), Slice("key3"), Slice("value3"));
  assert(s.ok());
  s = db->Put(WriteOptions(), Slice("key4"), Slice("destroy"));
  assert(s.ok());

  std::cout << "[" << main_tid
            << "] main       : Initiating a non-blocking manual compaction\n";

  // Prepare the compaction options.
  // Set async_completion_cb to have it non-blocking
  CompactRangeOptions cro;
  auto completion_cb = std::make_shared<CompactRangeCompleteCb>();
  cro.async_completion_cb = completion_cb;

  // Compacting up to "key4"
  Slice key4("key4");
  s = db->CompactRange(cro, nullptr, &key4);
  assert(s.ok());

  // Simulating work done while manual compaction proceeds asynchronously
  std::cout << "[" << main_tid
            << "] main       : Non-Blocking - I can continue while compaction "
               "occurs in the background\n";
  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::cout << "[" << main_tid
            << "] main       : Waiting for the non-blocking manual compaction "
               "to complete\n";
  auto completion_cb_future = completion_cb->GetFuture();
  auto future_wait_status =
      completion_cb_future.wait_for(std::chrono::seconds(5));
  assert(future_wait_status == std::future_status::ready);

  auto compact_range_completion_status = completion_cb_future.get();
  std::cout
      << "[" << main_tid
      << "] main       : Non-Blocking CompactRange() Completed with status="
      << compact_range_completion_status.ToString() << "\n";
  assert(compact_range_completion_status.ok());

  // Verify compaction results. Expecting the compaction filter to remove all
  // keys except "key3"
  Iterator* itr = db->NewIterator(ReadOptions());
  itr->SeekToFirst();
  assert(itr->Valid());
  assert("key3" == itr->key().ToString());

  itr->Next();
  assert(itr->Valid() == false);

  // Cleanup
  delete itr;
  delete options.compaction_filter;

  s = db->Close();
  assert(s.ok());
  delete db;

  return 0;
}
