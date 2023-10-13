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

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.Environment;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

import static org.assertj.core.api.Assertions.assertThat;

public class NativeLibraryLoaderTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void tempFolder() throws IOException {
    NativeLibraryLoader.getInstance().loadLibraryFromJarToTemp(
        temporaryFolder.getRoot().getAbsolutePath());
    final Path path = Paths.get(
        temporaryFolder.getRoot().getAbsolutePath(), Environment.getJniLibraryFileName("speedb"));
    assertThat(Files.exists(path)).isTrue();
    assertThat(Files.isReadable(path)).isTrue();
  }

  @Test
  public void overridesExistingLibrary() throws IOException {
    File first = NativeLibraryLoader.getInstance().loadLibraryFromJarToTemp(
        temporaryFolder.getRoot().getAbsolutePath());
    NativeLibraryLoader.getInstance().loadLibraryFromJarToTemp(
        temporaryFolder.getRoot().getAbsolutePath());
    assertThat(first.exists()).isTrue();
  }
}
