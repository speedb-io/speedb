# Copyright (C) 2022 Speedb Ltd. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

speedb_SOURCES = \
     speedb_registry.cc                            \
     paired_filter/speedb_paired_bloom.cc          \
     paired_filter/speedb_paired_bloom_internal.cc \
     pinning_policy/scoped_pinning_policy.cc       \


speedb_FUNC = register_SpeedbPlugins

speedb_HEADERS = \
     paired_filter/speedb_paired_bloom.h           \
     pinning_policy/scoped_pinning_policy.h        \

speedb_TESTS =   \
     speedb_customizable_test.cc                   \
     pinning_policy/scoped_pinning_policy_test.cc  \

speedb_TESTS =                                     \
     speedb_customizable_test.cc				 \

speedb_JAVA_TESTS = org.rocksdb.SpeedbFilterTest   \
