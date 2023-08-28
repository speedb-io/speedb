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

  Status ToVector(const std::string& /*opts_str*/, char /*delim*/,
                  std::vector<std::string>* /*elems*/) const override {
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
  Status ToVector(const std::string& opts_str, char delim,
                  std::vector<std::string>* elems) const override;

 protected:
  // Returns the next token marked by the delimiter from "opts" after start in
  // token and updates end to point to where that token stops. Delimiters inside
  // of braces are ignored. Returns OK if a token is found and an error if the
  // input opts string is mis-formatted.
  // Given "a=AA;b=BB;" start=2 and delimiter=";", token is "AA" and end points
  // to "b" Given "{a=A;b=B}", the token would be "a=A;b=B"
  //
  // @param opts The string in which to find the next token
  // @param delimiter The delimiter between tokens
  // @param start     The position in opts to start looking for the token
  // @param ed        Returns the end position in opts of the token
  // @param token     Returns the token
  // @returns OK if a token was found
  // @return InvalidArgument if the braces mismatch
  //          (e.g. "{a={b=c;}" ) -- missing closing brace
  // @return InvalidArgument if an expected delimiter is not found
  //        e.g. "{a=b}c=d;" -- missing delimiter before "c"
  Status NextToken(const std::string& opts, char delimiter, size_t start,
                   size_t* end, std::string* token) const;

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
