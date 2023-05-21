#pragma once

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
using Validator = std::function<bool(const void* /*addr*/)>;

class UseCaseConfig {
 public:
  explicit UseCaseConfig(const Validator& vf) : validator_(vf) {}

  UseCaseConfig(const UseCaseConfig& other) : validator_(other.validator_) {}

  UseCaseConfig& operator=(const UseCaseConfig& other) {
    if (this != &other) {
      validator_ = other.validator_;
    }

    return *this;
  }

  bool IsValid(const void* addr) const {
    return validator_ && validator_(addr);
  }

  UseCaseConfig& SetValidator(const Validator& vf) {
    validator_ = vf;
    return *this;
  }

  template <typename T>
  static UseCaseConfig Range(const T& min, const T& max) {
    return UseCaseConfig([min, max](const void* addr) {
      const auto value = static_cast<const T*>(addr);
      return (*value >= min) && (*value <= max);
    });
  }

  template <typename T>
  static UseCaseConfig Choice(const std::vector<T>& choices) {
    return UseCaseConfig([choices](const void* addr) {
      const auto value = static_cast<const T*>(addr);
      return std::find(choices.begin(), choices.end(), *value) != choices.end();
    });
  }

  template <typename T>
  static UseCaseConfig Equals(const T& expected_value) {
    return UseCaseConfig([expected_value](const void* addr) {
      const auto value = static_cast<const T*>(addr);
      return *value == expected_value;
    });
  }

 private:
  Validator validator_;
};

class UseCase : public Customizable {
 public:
  virtual ~UseCase() = default;
  static const char* Type() { return "UseCase"; }
  const char* Name() const override = 0;
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& id,
                                 std::shared_ptr<UseCase>* result);
  static Status ValidateOptions(const ConfigOptions& cfg_opts,
                                const std::string& validate_against,
                                const DBOptions& db_opts,
                                std::set<std::string>& valid_opts,
                                std::set<std::string>& invalid_opts);
  static Status ValidateOptions(const ConfigOptions& cfg_opts,
                                const std::string& validate_against,
                                const ColumnFamilyOptions& cf_opts,
                                std::set<std::string>& valid_opts,
                                std::set<std::string>& invalid_opts);
  static Status ValidateOptions(const ConfigOptions& cfg_opts,
                                const std::string& validate_against,
                                const Options& opts,
                                std::set<std::string>& valid_opts,
                                std::set<std::string>& invalid_opts);
  virtual Status Populate(const ConfigOptions& cfg_opts,
                          DBOptions& db_opts) = 0;
  virtual Status Populate(const ConfigOptions& cfg_opts,
                          ColumnFamilyOptions& cf_opts) = 0;
  virtual bool Validate(const ConfigOptions& cfg_opts, const DBOptions& db_opts,
                        std::set<std::string>& valid_opts,
                        std::set<std::string>& invalid_opts);
  virtual bool Validate(const ConfigOptions& cfg_opts,
                        const ColumnFamilyOptions& cf_opts,
                        std::set<std::string>& valid_opts,
                        std::set<std::string>& invalid_opts);
  virtual bool Validate(const ConfigOptions& cfg_opts, const Options& opts,
                        std::set<std::string>& valid_opts,
                        std::set<std::string>& invalid_opts);

 protected:
  void RegisterUseCaseDBOptionsConfig(
      std::unordered_map<std::string, UseCaseConfig>* config);

  void RegisterUseCaseCFOptionsConfig(
      std::unordered_map<std::string, UseCaseConfig>* config);

 private:
  std::vector<std::unordered_map<std::string, UseCaseConfig>*> uses_db_options_;
  std::vector<std::unordered_map<std::string, UseCaseConfig>*> uses_cf_options_;
};

}  // namespace ROCKSDB_NAMESPACE
