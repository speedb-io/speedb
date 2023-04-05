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

#include <string>
#pragma once

#define SPEEDB_MAJOR 2
#define SPEEDB_MINOR 5
#define SPEEDB_PATCH 0

namespace ROCKSDB_NAMESPACE {

// Returns the current version of Speedb as a string (e.g. "1.5.0").
// If with_patch is true, the patch is included (1.5.x).
// Otherwise, only major and minor version is included (1.5)
std::string GetSpeedbVersionAsString(bool with_patch = true);

}  // namespace ROCKSDB_NAMESPACE
