// TODO: ADD Speedb's Copyright Notice !!!!!

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

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE {

class SpdbTestCustomizable : public Customizable {
 public:
  SpdbTestCustomizable(const std::string& name) : name_(name) {}
  // Method to allow CheckedCast to work for this class
  static const char* kClassName() { return "SpdbTestCustomizable"; }

  const char* Name() const override { return name_.c_str(); }
  static const char* Type() { return "test.custom"; }
  bool IsInstanceOf(const std::string& name) const override {
    if (name == kClassName()) {
      return true;
    } else {
      return Customizable::IsInstanceOf(name);
    }
  }

 protected:
  const std::string name_;
};

namespace {

#ifndef ROCKSDB_LITE
static int RegisterLocalObjects(ObjectLibrary& library,
                                const std::string& /*arg*/) {
  size_t num_types;
  library.AddFactory<const FilterPolicy>(
      SpdbPairedBloomFilterPolicy::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<const FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(new SpdbPairedBloomFilterPolicy(23.2));
        return guard->get();
      });

  return static_cast<int>(library.GetFactoryCount(&num_types));
}
#endif  // !ROCKSDB_LITE
}  // namespace

class LoadCustomizableTest : public testing::Test {
 public:
  LoadCustomizableTest() {
    config_options_.ignore_unsupported_options = false;
    config_options_.invoke_prepare_options = false;
  }
  bool RegisterTests(const std::string& arg) {
#ifndef ROCKSDB_LITE
    config_options_.registry->AddLibrary("custom-tests",
                                         test::RegisterTestObjects, arg);
    config_options_.registry->AddLibrary("local-tests", RegisterLocalObjects,
                                         arg);
    return true;
#else
    (void)arg;
    return false;
#endif  // !ROCKSDB_LITE
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

#ifndef ROCKSDB_LITE
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
#endif  // ROCKSDB_LITE
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
