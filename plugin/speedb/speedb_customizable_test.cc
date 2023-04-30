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

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <memory>

#include "db/db_test_util.h"
#include "plugin/speedb/paired_filter/speedb_paired_bloom.h"
#include "port/stack_trace.h"
#include "rocksdb/customizable.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/filter_policy_internal.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

#ifdef GFLAGS
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE {

class LoadCustomizableTest : public testing::Test {
 public:
  LoadCustomizableTest() {
    config_options_.ignore_unsupported_options = false;
    config_options_.invoke_prepare_options = false;
  }
  bool RegisterTests(const std::string& arg) {
    (void)arg;
    return false;
  }

 protected:
  DBOptions db_opts_;
  ColumnFamilyOptions cf_opts_;
  ConfigOptions config_options_;
};

// ==========================================================================================
TEST_F(LoadCustomizableTest, LoadSpdbPairedFilterPolicyTest) {
  std::shared_ptr<TableFactory> table;
  std::shared_ptr<const FilterPolicy> result;
  ASSERT_NOK(FilterPolicy::CreateFromString(
      config_options_, SpdbPairedBloomFilterPolicy::kClassName(), &result));

  ASSERT_OK(FilterPolicy::CreateFromString(config_options_, "", &result));
  ASSERT_EQ(result, nullptr);
  ASSERT_OK(FilterPolicy::CreateFromString(
      config_options_, ReadOnlyBuiltinFilterPolicy::kClassName(), &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), ReadOnlyBuiltinFilterPolicy::kClassName());

  std::string table_opts = "id=BlockBasedTable; filter_policy=";
  ASSERT_OK(TableFactory::CreateFromString(config_options_,
                                           table_opts + "nullptr", &table));
  ASSERT_NE(table.get(), nullptr);
  auto bbto = table->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_EQ(bbto->filter_policy.get(), nullptr);
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_, table_opts + ReadOnlyBuiltinFilterPolicy::kClassName(),
      &table));
  bbto = table->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_NE(bbto->filter_policy.get(), nullptr);
  ASSERT_STREQ(bbto->filter_policy->Name(),
               ReadOnlyBuiltinFilterPolicy::kClassName());
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_, table_opts + SpdbPairedBloomFilterPolicy::kClassName(),
      &table));
  bbto = table->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_EQ(bbto->filter_policy.get(), nullptr);
  if (RegisterTests("Test")) {
    ASSERT_OK(FilterPolicy::CreateFromString(
        config_options_, SpdbPairedBloomFilterPolicy::kClassName(), &result));
    ASSERT_NE(result, nullptr);
    ASSERT_STREQ(result->Name(), SpdbPairedBloomFilterPolicy::kClassName());
    ASSERT_OK(TableFactory::CreateFromString(
        config_options_, table_opts + SpdbPairedBloomFilterPolicy::kClassName(),
        &table));
    bbto = table->GetOptions<BlockBasedTableOptions>();
    ASSERT_NE(bbto, nullptr);
    ASSERT_NE(bbto->filter_policy.get(), nullptr);
    ASSERT_STREQ(bbto->filter_policy->Name(),
                 SpdbPairedBloomFilterPolicy::kClassName());
  }
}

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
