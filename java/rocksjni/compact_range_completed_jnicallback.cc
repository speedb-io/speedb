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
// This file implements the callback "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::CompactRangeCbIf.

#include "rocksjni/compact_range_completed_jnicallback.h"

#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {
CompactRangeCompletedJniCallback::CompactRangeCompletedJniCallback(
    JNIEnv* env, jobject jcompletion_cb)
    : JniCallback(env, jcompletion_cb) {
  InitCallbackMethodId(
      m_cb_mid, env,
      AbstractCompactRangeCompletedCbJni::getCompletedCbProxyMethodId);
}

void CompactRangeCompletedJniCallback::CompletedCb(Status completion_status) {
  if (m_cb_mid == nullptr) {
    return;
  }

  JNIEnv* env;
  jboolean attached_thread;
  jobject jcompletion_status = SetupCallbackInvocation<Status>(
      env, attached_thread, completion_status, StatusJni::construct);

  if (jcompletion_status != nullptr) {
    env->CallVoidMethod(m_jcallback_obj, m_cb_mid, jcompletion_status);
  }

  CleanupCallbackInvocation(env, attached_thread, {&jcompletion_status});
}

void CompactRangeCompletedJniCallback::InitCallbackMethodId(
    jmethodID& mid, JNIEnv* env, jmethodID (*get_id)(JNIEnv* env)) {
  mid = get_id(env);
}

template <class T>
jobject CompactRangeCompletedJniCallback::SetupCallbackInvocation(
    JNIEnv*& env, jboolean& attached_thread, const T& cpp_obj,
    jobject (*convert)(JNIEnv* env, const T* cpp_obj)) {
  attached_thread = JNI_FALSE;
  env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  return convert(env, &cpp_obj);
}

void CompactRangeCompletedJniCallback::CleanupCallbackInvocation(
    JNIEnv* env, jboolean attached_thread,
    std::initializer_list<jobject*> refs) {
  for (auto* ref : refs) {
    if (*ref == nullptr) continue;
    env->DeleteLocalRef(*ref);
  }

  if (env->ExceptionCheck()) {
    // exception thrown from CallVoidMethod
    env->ExceptionDescribe();  // print out exception to stderr
  }

  releaseJniEnv(attached_thread);
}

}  // namespace ROCKSDB_NAMESPACE
