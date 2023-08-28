// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>
#include <unordered_map>
#include <vector>

#include "rocksdb/customizable.h"

namespace ROCKSDB_NAMESPACE {

class OptionsFormatter : public Customizable {
 public:
  static const std::shared_ptr<OptionsFormatter>& Default();
  // Creates and configures a new OptionsFormatter from the input options and
  // id.
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::shared_ptr<OptionsFormatter>* result);

  static const char* Type() { return "OptionsFormatter"; }
  // Converts the map of options to a single string representation
  virtual std::string ToString(
      const std::string& prefix,
      const std::unordered_map<std::string, std::string>& options) const = 0;

  // Converts the string representation into a map of name/value options
  virtual Status ToMap(
      const std::string& opts_str,
      std::unordered_map<std::string, std::string>* opts_map) const = 0;

  // Converts the vector options to a single string representation
  virtual std::string ToString(const std::string& prefix, char separator,
                               const std::vector<std::string>& elems) const = 0;

  // Converts the string representation into vector of elements based on the
  // separator
  virtual Status ToVector(const std::string& opts_str, char separator,
                          std::vector<std::string>* elems) const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
