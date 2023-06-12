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

#ifndef ROCKSDB_LITE

#include "plugin/speedb/pinning_policy/scoped_pinning_policy.h"

#include <unordered_map>

#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, OptionTypeInfo>
    scoped_pinning_type_info = {
        {"capacity",
         {offsetof(struct ScopedPinningOptions, capacity), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"bottom_percent",
         {offsetof(struct ScopedPinningOptions, bottom_percent),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"mid_percent",
         {offsetof(struct ScopedPinningOptions, mid_percent),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

ScopedPinningPolicy::ScopedPinningPolicy() {
  RegisterOptions(&options_, &scoped_pinning_type_info);
}

ScopedPinningPolicy::ScopedPinningPolicy(const ScopedPinningOptions& options)
    : options_(options) {
  RegisterOptions(&options_, &scoped_pinning_type_info);
}

std::string ScopedPinningPolicy::GetId() const {
  return GenerateIndividualId();
}

bool ScopedPinningPolicy::CheckPin(const TablePinningOptions& tpo,
                                   uint8_t /* type */, size_t size,
                                   size_t usage) const {
  auto proposed = usage + size;
  if (tpo.is_bottom && options_.bottom_percent > 0) {
    if (proposed > (options_.capacity * options_.bottom_percent / 100)) {
      return false;
    }
  } else if (tpo.level > 0 && options_.mid_percent > 0) {
    if (proposed > (options_.capacity * options_.mid_percent / 100)) {
      return false;
    }
  } else if (proposed > options_.capacity) {
    return false;
  }

  return true;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
