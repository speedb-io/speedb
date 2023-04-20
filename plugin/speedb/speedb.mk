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

speedb_SOURCES = 																			\
		   speedb_registry.cc															\
		   memtable/hash_spd_rep.cc	        							\
 			 paired_filter/speedb_paired_bloom.cc						\
 			 paired_filter/speedb_paired_bloom_internal.cc	\


speedb_FUNC = register_SpeedbPlugins

speedb_HEADERS = 																  								\
										paired_filter/speedb_paired_bloom.h						\

speedb_TESTS = 																										\
     speedb_customizable_test.cc																	\
		 paired_filter/speedb_db_bloom_filter_test.cc									\

speedb_JAVA_TESTS = org.rocksdb.SpeedbFilterTest \
