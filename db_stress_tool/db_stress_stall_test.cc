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

#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
// #include "file/file_util.h"

namespace ROCKSDB_NAMESPACE {
class StallStressTest : public StressTest {
 public:
  StallStressTest() {}

  ~StallStressTest() override {}

  bool IsStateTracked() const override { return false; }

  void InitDb(SharedState* shared) override {
    uint64_t now = clock_->NowMicros();
    fprintf(stdout, "%s Initializing db_stress\n",
            clock_->TimeToString(now / 1000000).c_str());
    PrintEnv();
    OpenStallTest();
    BuildOptionsTable();
  }

  void OpenStallTest() {
    InitializeOptionsFromFlags(cache_, compressed_cache_, filter_policy_,
                               options_);

    InitializeOptionsGeneral(cache_, compressed_cache_, filter_policy_,
                             options_);
  }

  void OperateDb(ThreadState* thread) override {}

  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& /* read_opts */,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys,
                 char (&value)[100]) override {
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    uint64_t value_base = batch_id_.fetch_add(1);
    size_t sz =
        GenerateValue(static_cast<uint32_t>(value_base), value, sizeof(value));
    Slice v(value, sz);
    WriteBatch batch;
    for (auto cf : rand_column_families) {
      ColumnFamilyHandle* cfh = column_families_[cf];
      if (FLAGS_use_merge) {
        batch.Merge(cfh, key, v);
      } else { /* !FLAGS_use_merge */
        batch.Put(cfh, key, v);
      }
    }
    Status s = db_->Write(write_opts, &batch);
    if (!s.ok()) {
      fprintf(stderr, "multi put or merge error: %s\n", s.ToString().c_str());
      thread->stats.AddErrors(1);
    } else {
      auto num = static_cast<long>(rand_column_families.size());
      thread->stats.AddBytesForWrites(num, (sz + 1) * num);
    }

    return s;
  }

  Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                    const std::vector<int>& rand_column_families,
                    const std::vector<int64_t>& rand_keys) override {
    fprintf(stderr, "StallStressTest does not support TestDelete\n");
    assert(false);
    std::terminate();
  }

  Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys) override {
    fprintf(stderr, "StallStressTest does not support TestDeleteRange\n");
    assert(false);
    std::terminate();
  }

  void TestIngestExternalFile(
      ThreadState* /* thread */,
      const std::vector<int>& /* rand_column_families */,
      const std::vector<int64_t>& /* rand_keys */) override {
    fprintf(stderr,
            "StallStressTest does not support TestIngestExternalFile\n");
    assert(false);
    std::terminate();
  }

  Status TestGet(ThreadState* thread, const ReadOptions& readoptions,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys) override {
    fprintf(stderr, "StallStressTest does not support TestGet\n");
    assert(false);
    std::terminate();
  }

  std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override {
    fprintf(stderr, "StallStressTest does not support TestMultiGet\n");
    assert(false);
    std::terminate();
  }

  Status TestPrefixScan(ThreadState* thread, const ReadOptions& readoptions,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override {
    fprintf(stderr, "StallStressTest does not support TestPrefixScan\n");
    assert(false);
    std::terminate();
  }

  void VerifyDb(ThreadState* thread) const override {
    fprintf(stderr, "StallStressTest does not support VerifyDb\n");
    assert(false);
    std::terminate();
  }

#ifndef ROCKSDB_LITE
  void ContinuouslyVerifyDb(ThreadState* thread) const override {
    fprintf(stderr, "StallStressTest does not support ContinuouslyVerifyDb\n");
    assert(false);
    std::terminate();
  }
#else   // ROCKSDB_LITE
  void ContinuouslyVerifyDb(ThreadState* /*thread*/) const override {}
#endif  // !ROCKSDB_LITE

  std::vector<int> GenerateColumnFamilies(
      const int /* num_column_families */,
      int /* rand_column_family */) const override {
    std::vector<int> ret;
    int num = static_cast<int>(column_families_.size());
    int k = 0;
    std::generate_n(back_inserter(ret), num, [&k]() -> int { return k++; });
    return ret;
  }

  ColumnFamilyHandle* GetControlCfh(ThreadState* thread,
                                    int /*column_family_id*/
                                    ) override {
    // All column families should contain the same data. Randomly pick one.
    return column_families_[thread->rand.Next() % column_families_.size()];
  }

 private:
  std::atomic<int64_t> batch_id_;
};

StressTest* CreateStallStressTest() { return new StallStressTest(); }

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
