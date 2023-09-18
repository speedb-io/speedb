// Copyright (C) 2023 Speedb Ltd. All rights reserved.
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
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::EventListener.

#include <jni.h>

#include <memory>

#include "include/org_rocksdb_AbstractCompactRangeCompletedCb.h"
#include "rocksdb/options.h"
#include "rocksjni/compact_range_completed_jnicallback.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_AbstractCompactRangeCompletedCb
 * Method:    createNewCompactRangeCompletedCb
 * Signature: (J)J
 */
jlong Java_org_rocksdb_AbstractCompactRangeCompletedCb_createNewCompactRangeCompletedCb(
    JNIEnv* env, jobject jobj) {
  auto* sptr_completion_cb =
      new std::shared_ptr<ROCKSDB_NAMESPACE::CompactRangeCompletedCbIf>(
          new ROCKSDB_NAMESPACE::CompactRangeCompletedJniCallback(env, jobj));
  return GET_CPLUSPLUS_POINTER(sptr_completion_cb);
}

/*
 * Class:     org_rocksdb_AbstractCompactRangeCompletedCb
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractCompactRangeCompletedCb_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  delete reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::CompactRangeCompletedJniCallback>*>(
      jhandle);
}
