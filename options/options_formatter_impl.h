// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/utilities/options_formatter.h"

namespace ROCKSDB_NAMESPACE {
class LogOptionsFormatter : public OptionsFormatter {
 public:
  static const char* kClassName() { return "LogOptionsFormatter"; }
  const char* Name() const override { return kClassName(); }
  static const char* kNickName() { return "Log"; }
  const char* NickName() const override { return kNickName(); }
  std::string ToString(const std::string& prefix,
                       const std::unordered_map<std::string, std::string>&
                           options) const override;
  std::string ToString(const std::string& prefix, char separator,
                       const std::vector<std::string>& elems) const override;
  Status ToMap(const std::string& /*opts_str*/,
               std::unordered_map<std::string, std::string>* /*opts_map*/)
      const override {
    return Status::NotSupported();
  }

 protected:
  void AppendElem(const std::string& prefix, const std::string& name,
                  const std::string& value, std::string* result) const;
};

class DefaultOptionsFormatter : public OptionsFormatter {
 public:
  static const char* kClassName() { return "DefaultOptionsFormatter"; }
  const char* Name() const override { return kClassName(); }
  static const char* kNickName() { return "Default"; }
  const char* NickName() const override { return kNickName(); }

  std::string ToString(const std::string& prefix,
                       const std::unordered_map<std::string, std::string>&
                           options) const override;
  Status ToMap(
      const std::string& opts_str,
      std::unordered_map<std::string, std::string>* opts_map) const override;
  std::string ToString(const std::string& prefix, char separator,
                       const std::vector<std::string>& elems) const override;

 protected:
  void AppendElem(const std::string& name, const std::string& value,
                  std::string* result) const;
};

class PropertiesOptionsFormatter : public DefaultOptionsFormatter {
 public:
  static const char* kClassName() { return "PropertiesOptionsFormatter"; }
  const char* Name() const override { return kClassName(); }
  static const char* kNickName() { return "Properties"; }
  const char* NickName() const override { return kNickName(); }
  std::string ToString(const std::string& prefix,
                       const std::unordered_map<std::string, std::string>&
                           options) const override;
  Status ToMap(const std::string& /*opts_str*/,
               std::unordered_map<std::string, std::string>* /*opts_map*/)
      const override;
};

}  // namespace ROCKSDB_NAMESPACE
