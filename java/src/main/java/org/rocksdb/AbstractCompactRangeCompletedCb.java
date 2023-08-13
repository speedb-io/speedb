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

package org.rocksdb;

/**
 */
public abstract class AbstractCompactRangeCompletedCb
    extends RocksCallbackObject implements CompactRangeCompletedCb {
  @Override
  public void CompactRangeCompleted(final Status completionStatus) {
    // no-op
  }

  /**
   * Called from JNI, proxy for
   *     {@link #CompactRangeCompleted(Status)}.
   *
   * @param completion_status the completion status
   */
  private void compactRangeCompletedCbProxy(final Status completion_status) {
    CompactRangeCompleted(completion_status);
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewCompactRangeCompletedCb();
  }

  /**
   * Deletes underlying C++ native callback object pointer
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native long createNewCompactRangeCompletedCb();
  private native void disposeInternal(final long handle);
}
