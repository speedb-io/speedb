//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "rocksdb/utilities/options_formatter.h"

#include <algorithm>
#include <memory>

#include "options/options_formatter_impl.h"
#include "options/options_parser.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
void DefaultOptionsFormatter::AppendElem(const std::string& name,
                                         const std::string& value,
                                         std::string* result) const {
  result->append(name);
  result->append("=");
  if (value.find('=') != std::string::npos && value[0] != '{') {
    result->append("{" + value + "}");
  } else {
    result->append(value);
  }
}

std::string DefaultOptionsFormatter::ToString(
    const std::string& /*prefix*/,
    const std::unordered_map<std::string, std::string>& options) const {
  std::string result;
  std::string id;
  for (const auto& it : options) {
    if (it.first == OptionTypeInfo::kIdPropName()) {
      id = it.second;
    } else {
      if (!result.empty()) {
        result.append(";");
      }
      AppendElem(it.first, it.second, &result);
    }
  }
  if (id.empty()) {
    return result;
  } else if (result.empty()) {
    return id;
  } else {
    std::string id_string;
    AppendElem(OptionTypeInfo::kIdPropName(), id, &id_string);
    return id_string + ";" + result;
  }
}

Status DefaultOptionsFormatter::ToMap(
    const std::string& opts_str,
    std::unordered_map<std::string, std::string>* opts_map) const {
  return StringToMap(opts_str, opts_map);
}

// Converts the vector options to a single string representation
std::string DefaultOptionsFormatter::ToString(
    const std::string& /*prefix*/, char separator,
    const std::vector<std::string>& elems) const {
  std::string result;
  int printed = 0;
  for (const auto& elem : elems) {
    if (printed++ > 0) {
      result += separator;
    }
    if (elem.find(separator) != std::string::npos) {
      // If the element contains embedded separators, put it inside of brackets
      result.append("{" + elem + "}");
    } else if (elem.find("=") != std::string::npos) {
      // If the element contains embedded options, put it inside of brackets
      result.append("{" + elem + "}");
    } else {
      result += elem;
    }
  }
  if (result.find("=") != std::string::npos) {
    return "{" + result + "}";
  } else if (printed > 1 && result.at(0) == '{') {
    return "{" + result + "}";
  } else {
    return result;
  }
}

Status DefaultOptionsFormatter::ToVector(
    const std::string& opts_str, char separator,
    std::vector<std::string>* elems) const {
  Status status;
  for (size_t start = 0, end = 0;
       status.ok() && start < opts_str.size() && end != std::string::npos;
       start = end + 1) {
    std::string token;
    status =
        OptionTypeInfo::NextToken(opts_str, separator, start, &end, &token);
    if (status.ok()) {
      elems->emplace_back(token);
    }
  }
  return status;
}

std::string PropertiesOptionsFormatter::ToString(
    const std::string& prefix,
    const std::unordered_map<std::string, std::string>& options) const {
  std::string result;
  std::string id;
  const char* separator = prefix.empty() ? "\n  " : "; ";
  for (const auto& it : options) {
    if (it.first == OptionTypeInfo::kIdPropName()) {
      id = it.second;
    } else {
      if (!result.empty()) {
        result.append(separator);
      }
      AppendElem(it.first, it.second, &result);
    }
  }
  if (id.empty()) {
    return result;
  } else if (result.empty()) {
    return id;
  } else {
    std::string id_string;
    AppendElem(OptionTypeInfo::kIdPropName(), id, &id_string);
    return id_string + separator + result;
  }
}

Status PropertiesOptionsFormatter::ToMap(
    const std::string& props,
    std::unordered_map<std::string, std::string>* map) const {
  if (props.find('\n') != std::string::npos) {
    size_t pos = 0;
    int line_num = 0;
    Status s;
    while (s.ok() && pos < props.size()) {
      size_t nl_pos = props.find('\n', pos);
      std::string name;
      std::string value;
      if (nl_pos == std::string::npos) {
        s = RocksDBOptionsParser::ParseStatement(&name, &value,
                                                 props.substr(pos), line_num);
        pos = props.size();
      } else {
        s = RocksDBOptionsParser::ParseStatement(
            &name, &value, props.substr(pos, nl_pos - pos), line_num);
        pos = nl_pos + 1;
      }
      if (s.ok()) {
        (*map)[name] = value;
        line_num++;
      }
    }
    return s;
  } else {
    return DefaultOptionsFormatter::ToMap(props, map);
  }
}
void LogOptionsFormatter::AppendElem(const std::string& prefix,
                                     const std::string& name,
                                     const std::string& value,
                                     std::string* result) const {
  if (!result->empty()) {
    result->append("\n");
  }
  result->append(prefix);
  result->append(name);
  result->append(": ");
  result->append(value);
}

std::string LogOptionsFormatter::ToString(
    const std::string& prefix,
    const std::unordered_map<std::string, std::string>& options) const {
  std::string result;
  if (!options.empty()) {
    const auto& id = options.find(OptionTypeInfo::kIdPropName());
    if (options.size() > 1) {
      std::string spaces = "  ";
      if (!prefix.empty()) {
        // Indent by the number of "." in the prefix
        for (int count = static_cast<int>(
                 std::count(prefix.begin(), prefix.end(), '.'));
             count >= 0; count--) {
          spaces.append("  ");
        }
      }
      if (id != options.end()) {
        AppendElem(spaces, id->first, id->second, &result);
      }
      for (const auto& it : options) {
        if (it.first != OptionTypeInfo::kIdPropName()) {
          AppendElem(spaces, it.first, it.second, &result);
        }
      }
    } else if (id != options.end()) {
      result = id->second;
    } else {
      const auto& it = options.begin();
      AppendElem(prefix, it->first, it->second, &result);
    }
  }
  return result;
}

std::string LogOptionsFormatter::ToString(
    const std::string& prefix, char /*separator*/,
    const std::vector<std::string>& elems) const {
  std::string result;
  int printed = 0;
  for (const auto& elem : elems) {
    result.append("  ");
    result.append(prefix);
    result.append("[" + std::to_string(printed++) + "]: ");
    result.append(elem);
    result.append("\n");
  }
  return result;
}

static int RegisterBuiltinOptionsFormatter(ObjectLibrary& library,
                                           const std::string& /*arg*/) {
  size_t num_types;
  library.AddFactory<OptionsFormatter>(
      ObjectLibrary::PatternEntry(DefaultOptionsFormatter::kClassName())
          .AnotherName(DefaultOptionsFormatter::kNickName()),
      [](const std::string& /*uri*/, std::unique_ptr<OptionsFormatter>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new DefaultOptionsFormatter());
        return guard->get();
      });
  library.AddFactory<OptionsFormatter>(
      ObjectLibrary::PatternEntry(PropertiesOptionsFormatter::kClassName())
          .AnotherName(PropertiesOptionsFormatter::kNickName()),
      [](const std::string& /*uri*/, std::unique_ptr<OptionsFormatter>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new PropertiesOptionsFormatter());
        return guard->get();
      });
  library.AddFactory<OptionsFormatter>(
      ObjectLibrary::PatternEntry(LogOptionsFormatter::kClassName())
          .AnotherName(LogOptionsFormatter::kNickName()),
      [](const std::string& /*uri*/, std::unique_ptr<OptionsFormatter>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new LogOptionsFormatter());
        return guard->get();
      });
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
const std::shared_ptr<OptionsFormatter>& OptionsFormatter::Default() {
  static std::shared_ptr<OptionsFormatter> default_formatter =
      std::make_shared<DefaultOptionsFormatter>();
  return default_formatter;
}

Status OptionsFormatter::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<OptionsFormatter>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinOptionsFormatter(*(ObjectLibrary::Default().get()), "");
  });
  return LoadSharedObject<OptionsFormatter>(config_options, value, result);
}

}  // namespace ROCKSDB_NAMESPACE
