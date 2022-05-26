#pragma once

#define SPEEDB_MAJOR 1
#define SPEEDB_MINOR 5
#define SPEEDB_PATCH 0

namespace ROCKSDB_NAMESPACE {
// Returns the current version of SpeeDB as a string (e.g. "1.5.0").
// If with_patch is true, the patch is included (1.5.x).
// Otherwise, only major and minor version is included (1.5)
std::string GetSpeedbVersionAsString(bool with_patch = true);
}  // namespace ROCKSDB_NAMESPACE
