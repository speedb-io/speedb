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
#include <memory>
#include <unordered_map>
#include <vector>

#include "rocksdb/customizable.h"

namespace ROCKSDB_NAMESPACE {
class OptionProperties;

// EXPERIMENTAL
// Class to create string representations of name/value pairs
// This class is an abstract class that can take name-value pairs and convert
// them to strings and (potentially) revert that process (strings into
// name-value pairs).  Currently, this class is used by the Options system to
// take the serialized versions of options and save them in different
// representations (such as the Options properties file).  This class could also
// be used to save these values in different formats, such as written to a LOG
// file or saved as JSON or XML objects.
//
// This class is currently experimental and the interfaces may need to be
// changed to support additional formats.
class OptionsFormatter : public Customizable {
 public:
  static const std::shared_ptr<OptionsFormatter>& Default();
  // Creates and configures a new OptionsFormatter from the input options and
  // id.
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::shared_ptr<OptionsFormatter>* result);

  static const char* Type() { return "OptionsFormatter"; }
  using Customizable::ToString;
  // Converts the map of properties to a single string representation
  virtual std::string ToString(const std::string& prefix,
                               const OptionProperties& props) const = 0;

  // Converts the string representation into a name/value properties
  virtual Status ToProps(const std::string& opts_str,
                         OptionProperties* props) const = 0;

  // Converts the vector to a single string representation
  virtual std::string ToString(const std::string& prefix, char separator,
                               const std::vector<std::string>& elems) const = 0;

  // Converts the string representation into vector of elements based on the
  // separator
  virtual Status ToVector(const std::string& opts_str, char separator,
                          std::vector<std::string>* elems) const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
