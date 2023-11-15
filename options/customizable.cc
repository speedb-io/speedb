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
//
// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/customizable.h"

#include <sstream>

#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

std::string Customizable::GetOptionName(const std::string& long_name) const {
  const std::string& name = Name();
  size_t name_len = name.size();
  if (long_name.size() > name_len + 1 &&
      long_name.compare(0, name_len, name) == 0 &&
      long_name.at(name_len) == '.') {
    return long_name.substr(name_len + 1);
  } else {
    return Configurable::GetOptionName(long_name);
  }
}

std::string Customizable::GenerateIndividualId() const {
  std::ostringstream ostr;
  ostr << Name() << "@" << static_cast<const void*>(this) << "#"
       << port::GetProcessID();
  return ostr.str();
}

Status Customizable::GetOption(const ConfigOptions& config_options,
                               const std::string& opt_name,
                               std::string* value) const {
  if (opt_name == OptionTypeInfo::kIdPropName()) {
    *value = GetId();
    return Status::OK();
  } else {
    return Configurable::GetOption(config_options, opt_name, value);
  }
}

Status Customizable::SerializeOptions(const ConfigOptions& config_options,
                                      const std::string& prefix,
                                      OptionProperties* props) const {
  Status s;
  auto id = GetId();
  props->insert({OptionTypeInfo::kIdPropName(), id});

  if (!config_options.IsShallow() && !id.empty()) {
    s = Configurable::SerializeOptions(config_options, prefix, props);
  }
  return s;
}

bool Customizable::AreEquivalent(const ConfigOptions& config_options,
                                 const Configurable* other,
                                 std::string* mismatch) const {
  if (config_options.sanity_level > ConfigOptions::kSanityLevelNone &&
      this != other) {
    const Customizable* custom = reinterpret_cast<const Customizable*>(other);
    if (custom == nullptr) {  // Cast failed
      return false;
    } else if (GetId() != custom->GetId()) {
      *mismatch = OptionTypeInfo::kIdPropName();
      return false;
    } else if (config_options.sanity_level >
               ConfigOptions::kSanityLevelLooselyCompatible) {
      bool matches =
          Configurable::AreEquivalent(config_options, other, mismatch);
      return matches;
    }
  }
  return true;
}

Status Customizable::GetOptionsMap(const ConfigOptions& config_options,
                                   const Customizable* customizable,
                                   const std::string& value, std::string* id,
                                   OptionProperties* props) {
  Status status;
  if (value.empty() || value == kNullptrString) {
    *id = "";
    props->clear();
  } else if (customizable != nullptr) {
    status = Configurable::GetOptionsMap(config_options, value,
                                         customizable->GetId(), id, props);
    if (status.ok() && customizable->IsInstanceOf(*id)) {
      // The new ID and the old ID match, so the objects are the same type.
      // Try to get the existing options, ignoring any errors
      OptionProperties current;
      if (ConfigurableHelper::SerializeOptions(config_options, *customizable,
                                               "", &current)
              .ok()) {
        props->insert(current.begin(), current.end());
      }
    }
  } else {
    status = Configurable::GetOptionsMap(config_options, value, "", id, props);
  }
  return status;
}

Status Customizable::ConfigureNewObject(
    const ConfigOptions& config_options, Customizable* object,
    const std::unordered_map<std::string, std::string>& opt_map) {
  Status status;
  if (object != nullptr) {
    status = object->ConfigureFromMap(config_options, opt_map);
  } else if (!opt_map.empty()) {
    status = Status::InvalidArgument("Cannot configure null object ");
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE
