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
                       const OptionProperties& props) const override;
  std::string ToString(const std::string& prefix, char separator,
                       const std::vector<std::string>& elems) const override;
  Status ToProps(const std::string& /*opts_str*/,
                 OptionProperties* /*props*/) const override {
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
                       const OptionProperties& props) const override;
  Status ToProps(const std::string& opts_str,
                 OptionProperties* props) const override;
  using OptionsFormatter::ToString;
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
  static const char* kNickName() { return "OptionProperties"; }
  const char* NickName() const override { return kNickName(); }
  using OptionsFormatter::ToString;
  std::string ToString(const std::string& prefix,
                       const OptionProperties& props) const override;
  Status ToProps(const std::string& /*opts_str*/,
                 OptionProperties* /*props*/) const override;
};

}  // namespace ROCKSDB_NAMESPACE
