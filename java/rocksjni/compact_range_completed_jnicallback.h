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

#pragma once

#include <jni.h>

#include <memory>
#include <set>

#include "rocksdb/options.h"
#include "rocksjni/jnicallback.h"

namespace ROCKSDB_NAMESPACE {

class CompactRangeCompletedJniCallback : public JniCallback,
                                         public CompactRangeCompletedCbIf {
 public:
  CompactRangeCompletedJniCallback(JNIEnv* env, jobject jcompletion_cb);
  virtual ~CompactRangeCompletedJniCallback() = default;

  void CompletedCb(Status completion_status) override;

 private:
  inline void InitCallbackMethodId(jmethodID& mid, JNIEnv* env,
                                   jmethodID (*get_id)(JNIEnv* env));
  template <class T>
  jobject SetupCallbackInvocation(JNIEnv*& env, jboolean& attached_thread,
                                  const T& cpp_obj,
                                  jobject (*convert)(JNIEnv* env,
                                                     const T* cpp_obj));

  void CleanupCallbackInvocation(JNIEnv* env, jboolean attached_thread,
                                 std::initializer_list<jobject*> refs);

  jmethodID m_cb_mid;
};

}  // namespace ROCKSDB_NAMESPACE
