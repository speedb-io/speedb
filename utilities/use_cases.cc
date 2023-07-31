#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "rocksdb/db_crashtest_use_case.h"
#include "rocksdb/options.h"
#include "rocksdb/use_case.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
Status ToUseCases(const ConfigOptions& cfg_opts, const std::string& value,
                  std::vector<std::shared_ptr<UseCase>>& use_cases) {
  Status status;
  for (size_t start = 0, end = 0;
       status.ok() && start < value.size() && end != std::string::npos;
       start = end + 1) {
    std::string token;
    status = OptionTypeInfo::NextToken(value, ',', start, &end, &token);
    if (status.ok()) {
      if (token.find('*') == std::string::npos) {
        std::shared_ptr<UseCase> use_case;
        status = UseCase::CreateFromString(cfg_opts, token, &use_case);
        if (status.ok() && use_case) {
          use_cases.push_back(use_case);
        }
      } else {
        // TODO: Pattern match on the factory names in factories to match the
        // token
        // std::vector<std::string> factories;
        // ObjectRegistry::Default()->GetFactoryNames(UseCase::Type(), &factories);
        // return bad status (some sort)
      }
    }
  }
  return status;
}

static int RegisterBuiltinDBCrashtestUseCases(ObjectLibrary& library,
                                              const std::string& arg) {
  library.AddFactory<UseCase>(
      SimpleDefaultParams::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<UseCase>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new SimpleDefaultParams());
        return guard->get();
      });
  library.AddFactory<UseCase>(
      TxnParams::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<UseCase>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new TxnParams());
        return guard->get();
      });
  library.AddFactory<UseCase>(
      BestEffortsRecoveryParams::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<UseCase>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new BestEffortsRecoveryParams());
        return guard->get();
      });
  library.AddFactory<UseCase>(
      BlobParams::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<UseCase>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new BlobParams());
        return guard->get();
      });
  library.AddFactory<UseCase>(
      TieredParams::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<UseCase>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new TieredParams());
        return guard->get();
      });
  library.AddFactory<DBCrashtestUseCase>(
      MultiopsTxnDefaultParams::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<DBCrashtestUseCase>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new MultiopsTxnDefaultParams());
        return guard->get();
      });
  return 1;
}

static int RegisterBuiltinUseCases(ObjectLibrary& library,
                                   const std::string& arg) {
  library.AddFactory<UseCase>(
      DBCrashtestUseCase::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<UseCase>* guard,
         std::string* /*errmsg*/) {
         guard->reset(new DBCrashtestUseCase());
        return guard->get();
      });
  RegisterBuiltinDBCrashtestUseCases(library, arg);
  return 1;
}

Status UseCase::CreateFromString(const ConfigOptions& cfg_opts,
                                 const std::string& value,
                                 std::shared_ptr<UseCase>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinUseCases(*(ObjectLibrary::Default().get()), "");
  });
  Status status =
      LoadSharedObject<UseCase>(cfg_opts, value, result);
  return status;
}

void UseCase::RegisterUseCaseDBOptionsConfig(
    std::unordered_map<std::string, UseCaseConfig>* config) {
  uses_db_options_.push_back(config);
}

void UseCase::RegisterUseCaseCFOptionsConfig(
    std::unordered_map<std::string, UseCaseConfig>* config) {
  uses_cf_options_.push_back(config);
}

bool UseCase::Validate(const ConfigOptions& cfg_opts, const DBOptions& db_opts,
                       std::set<std::string>& valid_opts,
                       std::set<std::string>& invalid_opts) {
  auto db_config = DBOptionsAsConfigurable(db_opts);
  return ConfigurableHelper::CheckUseCases(cfg_opts, *(db_config.get()),
                                           uses_db_options_, valid_opts,
                                           invalid_opts, nullptr) == 0;
}

bool UseCase::Validate(const ConfigOptions& cfg_opts,
                       const ColumnFamilyOptions& cf_opts,
                       std::set<std::string>& valid_opts,
                       std::set<std::string>& invalid_opts) {
  auto cf_config = CFOptionsAsConfigurable(cf_opts);
  return ConfigurableHelper::CheckUseCases(cfg_opts, *(cf_config.get()),
                                           uses_cf_options_, valid_opts,
                                           invalid_opts, nullptr) == 0;
}

bool UseCase::Validate(const ConfigOptions& cfg_opts, const Options& opts,
                       std::set<std::string>& valid_opts,
                       std::set<std::string>& invalid_opts) {
  DBOptions db_options(opts);
  ColumnFamilyOptions cf_options(opts);
  if (Validate(cfg_opts, db_options, valid_opts, invalid_opts) == 0) {
    return Validate(cfg_opts, cf_options, valid_opts, invalid_opts) == 0;
  } else {
    return false;
  }
}

Status UseCase::ValidateOptions(const ConfigOptions& cfg_opts,
                                const std::string& validate_against,
                                const DBOptions& db_opts,
                                std::set<std::string>& valid_opts,
                                std::set<std::string>& invalid_opts) {
  std::vector<std::shared_ptr<UseCase>> use_cases;
  Status s = ToUseCases(cfg_opts, validate_against, use_cases);
  if (s.ok()) {
    for (const auto& use_case : use_cases) {
      use_case->Validate(cfg_opts, db_opts, valid_opts, invalid_opts);
    }
    if (!invalid_opts.empty()) {
      s = Status::InvalidArgument();
    }
  }
  return s;
}

Status UseCase::ValidateOptions(const ConfigOptions& cfg_opts,
                                const std::string& validate_against,
                                const ColumnFamilyOptions& cf_opts,
                                std::set<std::string>& valid_opts,
                                std::set<std::string>& invalid_opts) {
  std::vector<std::shared_ptr<UseCase>> use_cases;
  Status s = ToUseCases(cfg_opts, validate_against, use_cases);
  if (s.ok()) {
    for (const auto& use_case : use_cases) {
      use_case->Validate(cfg_opts, cf_opts, valid_opts, invalid_opts);
    }
    if (!invalid_opts.empty()) {
      s = Status::InvalidArgument();
    }
  }
  return s;
}

Status UseCase::ValidateOptions(const ConfigOptions& cfg_opts,
                                const std::string& validate_against,
                                const Options& opts,
                                std::set<std::string>& valid_opts,
                                std::set<std::string>& invalid_opts) {
  std::vector<std::shared_ptr<UseCase>> use_cases;
  Status s = ToUseCases(cfg_opts, validate_against, use_cases);
  if (s.ok()) {
    for (const auto& use_case : use_cases) {
      use_case->Validate(cfg_opts, opts, valid_opts, invalid_opts);
    }
    if (!invalid_opts.empty()) {
      s = Status::InvalidArgument();
    }
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
