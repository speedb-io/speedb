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
        {"bottom_limit",
         {offsetof(struct ScopedPinningOptions, bottom_limit), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"mid_limit",
         {offsetof(struct ScopedPinningOptions, mid_limit), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
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
                                   size_t limit) const {
  if (tpo.is_bottom && options_.bottom_limit >= 0) {
    if (limit + size > (options_.capacity * options_.bottom_limit / 100)) {
      printf(
          "MJR: Rejecting pinned bottom size=%d/%d level=%d capacity=%d/%d\n",
          (int)size, (int)limit, tpo.level, (int)options_.capacity,
          (int)(options_.capacity * options_.bottom_limit / 100));
      return false;
    }
  } else if (tpo.level > 0 && options_.mid_limit >= 0) {
    if (limit + size > (options_.capacity * options_.mid_limit / 100)) {
      return false;
    }
  } else if (limit + size > options_.capacity) {
    return false;
  }

  return true;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
