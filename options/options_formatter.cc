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

#include "rocksdb/utilities/options_formatter.h"

#include <algorithm>
#include <memory>

#include "options/options_formatter_impl.h"
#include "options/options_parser.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

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
    const std::string& /*prefix*/, const OptionProperties& props) const {
  std::string result;
  std::string id;
  for (const auto& it : props) {
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

Status DefaultOptionsFormatter::NextToken(const std::string& opts,
                                          char delimiter, size_t pos,
                                          size_t* end,
                                          std::string* token) const {
  while (pos < opts.size() && isspace(opts[pos])) {
    ++pos;
  }
  // Empty value at the end
  if (pos >= opts.size()) {
    *token = "";
    *end = std::string::npos;
    return Status::OK();
  } else if (opts[pos] == '{') {
    int count = 1;
    size_t brace_pos = pos + 1;
    while (brace_pos < opts.size()) {
      if (opts[brace_pos] == '{') {
        ++count;
      } else if (opts[brace_pos] == '}') {
        --count;
        if (count == 0) {
          break;
        }
      }
      ++brace_pos;
    }
    // found the matching closing brace
    if (count == 0) {
      *token = trim(opts.substr(pos + 1, brace_pos - pos - 1));
      // skip all whitespace and move to the next delimiter
      // brace_pos points to the next position after the matching '}'
      pos = brace_pos + 1;
      while (pos < opts.size() && isspace(opts[pos])) {
        ++pos;
      }
      if (pos < opts.size() && opts[pos] != delimiter) {
        return Status::InvalidArgument("Unexpected chars after nested options");
      }
      *end = pos;
    } else {
      return Status::InvalidArgument(
          "Mismatched curly braces for nested options");
    }
  } else {
    *end = opts.find(delimiter, pos);
    if (*end == std::string::npos) {
      // It either ends with a trailing semi-colon or the last key-value pair
      *token = trim(opts.substr(pos));
    } else {
      *token = trim(opts.substr(pos, *end - pos));
    }
  }
  return Status::OK();
}

Status DefaultOptionsFormatter::ToProps(const std::string& opts_str,
                                        OptionProperties* props) const {
  static const char kDelim = ';';
  assert(props);
  // Example:
  //   opts_str = "write_buffer_size=1024;max_write_buffer_number=2;"
  //              "nested_opt={opt1=1;opt2=2};max_bytes_for_level_base=100"
  size_t pos = 0;
  std::string opts = trim(opts_str);
  // If the input string starts and ends with "{...}", strip off the brackets
  while (opts.size() > 2 && opts[0] == '{' && opts[opts.size() - 1] == '}') {
    opts = trim(opts.substr(1, opts.size() - 2));
  }

  while (pos < opts.size()) {
    size_t eq_pos = opts.find_first_of("={};", pos);
    if (eq_pos == std::string::npos) {
      return Status::InvalidArgument("Mismatched key value pair, '=' expected");
    } else if (opts[eq_pos] != '=') {
      return Status::InvalidArgument("Unexpected char in key");
    }

    std::string key = trim(opts.substr(pos, eq_pos - pos));
    if (key.empty()) {
      return Status::InvalidArgument("Empty key found");
    }

    std::string value;
    Status s = NextToken(opts, kDelim, eq_pos + 1, &pos, &value);
    if (!s.ok()) {
      return s;
    } else {
      (*props)[key] = value;
      if (pos == std::string::npos) {
        break;
      } else {
        pos++;
      }
    }
  }

  return Status::OK();
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

  // If the input string starts and ends with "{{...}}", strip off the outer
  // brackets
  std::string opts = opts_str;
  while (opts.size() > 4 && opts[0] == '{' && opts[1] == '{' &&
         opts[opts.size() - 2] == '}' && opts[opts.size() - 1] == '}') {
    opts = trim(opts.substr(1, opts.size() - 2));
  }

  for (size_t start = 0, end = 0;
       status.ok() && start < opts.size() && end != std::string::npos;
       start = end + 1) {
    std::string token;
    status = NextToken(opts, separator, start, &end, &token);
    if (status.ok()) {
      elems->emplace_back(token);
    }
  }
  return status;
}

std::string PropertiesOptionsFormatter::ToString(
    const std::string& prefix, const OptionProperties& props) const {
  std::string result;
  std::string id;
  const char* separator = prefix.empty() ? "\n  " : "; ";
  for (const auto& it : props) {
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

Status PropertiesOptionsFormatter::ToProps(const std::string& props_str,
                                           OptionProperties* props) const {
  if (props_str.find('\n') != std::string::npos) {
    size_t pos = 0;
    int line_num = 0;
    Status s;
    while (s.ok() && pos < props_str.size()) {
      size_t nl_pos = props_str.find('\n', pos);
      std::string name;
      std::string value;
      if (nl_pos == std::string::npos) {
        s = RocksDBOptionsParser::ParseStatement(
            &name, &value, props_str.substr(pos), line_num);
        pos = props_str.size();
      } else {
        s = RocksDBOptionsParser::ParseStatement(
            &name, &value, props_str.substr(pos, nl_pos - pos), line_num);
        pos = nl_pos + 1;
      }
      if (s.ok()) {
        (*props)[name] = value;
        line_num++;
      }
    }
    return s;
  } else {
    return DefaultOptionsFormatter::ToProps(props_str, props);
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

std::string LogOptionsFormatter::ToString(const std::string& prefix,
                                          const OptionProperties& props) const {
  std::string result;
  if (!props.empty()) {
    const auto& id = props.find(OptionTypeInfo::kIdPropName());
    if (props.size() > 1) {
      std::string spaces = "  ";
      if (!prefix.empty()) {
        // Indent by the number of "." in the prefix
        for (auto count = std::count(prefix.begin(), prefix.end(), '.');
             count >= 0; count--) {
          spaces.append("  ");
        }
      }
      if (id != props.end()) {
        AppendElem(spaces, id->first, id->second, &result);
      }
      for (const auto& it : props) {
        if (it.first != OptionTypeInfo::kIdPropName()) {
          AppendElem(spaces, it.first, it.second, &result);
        }
      }
    } else if (id != props.end()) {
      result = id->second;
    } else {
      const auto& it = props.begin();
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
