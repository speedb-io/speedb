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

# 3.12 is needed for FindPython
cmake_minimum_required(VERSION 3.12)

# Choose the amount of tests to run in parallel if CTEST_PARALLEL_LEVEL wasn't set
if(NOT DEFINED ENV{CTEST_PARALLEL_LEVEL})
    # Compatibility with the Makefile: support the `J` environment variable
    if(DEFINED ENV{J} AND "$ENV{J}" GREATER 0)
        set(ENV{CTEST_PARALLEL_LEVEL} "$ENV{J}")
    else()
        include(ProcessorCount)
        ProcessorCount(NCPU)
        if(NOT NCPU EQUAL 0)
            set(ENV{CTEST_PARALLEL_LEVEL} ${NCPU})
        endif()
    endif()
endif()

# For Makefile compatibility try the following sequence if TEST_TMPDIR isn't set:
# * Use TMPD if set
# * Find a suitable base directory and create a temporary directory under it:
#   * /dev/shm on Linux if exists and has the sticky bit set
#   * TMPDIR if set and exists
#   * On Windows use TMP is set and exists
#   * On Windows use TEMP is set and exists
#   * /tmp if exists
if(NOT DEFINED ENV{TEST_TMPDIR})
    # Use TMPD if set
    if(DEFINED ENV{TMPD})
        set(test_dir "$ENV{TMPD}")
    else()
        # On Linux, use /dev/shm if the sticky bit is set
        if("${CMAKE_HOST_SYSTEM_NAME}" STREQUAL "Linux" AND IS_DIRECTORY "/dev/shm")
            execute_process(COMMAND test -k /dev/shm RESULT_VARIABLE status OUTPUT_QUIET ERROR_QUIET)
            if(status EQUAL 0)
                set(test_dir "/dev/shm")
            endif()
        endif()
        # Use TMPDIR as base if set
        if(NOT DEFINED test_dir AND IS_DIRECTORY "$ENV{TMPDIR}")
            set(test_dir "$ENV{TMPDIR}")
        elseif("${CMAKE_HOST_SYSTEM_NAME}" STREQUAL "Windows")
            # Use TMP or TEMP as base if set
            # See https://devblogs.microsoft.com/oldnewthing/20150417-00/?p=44213
            if(IS_DIRECTORY "$ENV{TMP}")
                set(test_dir "$ENV{TMP}")
            elseif(IS_DIRECTORY "$ENV{TEMP}")
                set(test_dir "$ENV{TEMP}")
            endif()
        endif()
        # Fall back to /tmp if exists
        if(NOT DEFINED test_dir AND IS_DIRECTORY "/tmp")
            set(test_dir "/tmp")
        endif()
        # Create a temporary directory under the base path that we determined
        if(DEFINED test_dir)
            include(FindPython)
            find_package(Python COMPONENTS Interpreter)
            # Try using Python for more portability when creating the temporary
            # sub-directory, but don't depend on it
            if(Python_Interpreter_FOUND)
                execute_process(
                    COMMAND "${CMAKE_COMMAND}" -E env "test_dir=${test_dir}"
                    "${Python_EXECUTABLE}" -c "import os, tempfile; print(tempfile.mkdtemp(prefix='rocksdb.', dir=os.environ['test_dir']))"
                    RESULT_VARIABLE status OUTPUT_VARIABLE tmpdir
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
                if (NOT status EQUAL 0)
                    message(FATAL_ERROR "Python mkdtemp failed")
                endif()
                set(test_dir "${tmpdir}")
            elseif(NOT "${CMAKE_HOST_SYSTEM_NAME}" STREQUAL "Windows")
                execute_process(
                    COMMAND mktemp -d "${test_dir}/rocksdb.XXXXXX"
                    RESULT_VARIABLE status OUTPUT_VARIABLE tmpdir
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
                if (NOT status EQUAL 0)
                    message(FATAL_ERROR "mkdtemp failed")
                endif()
                set(test_dir "${tmpdir}")
            endif()
        endif()
    endif()
    if(DEFINED test_dir)
        set(ENV{TEST_TMPDIR} "${test_dir}")
    endif()
endif()

if(DEFINED ENV{TEST_TMPDIR})
    message(STATUS "Running $ENV{CTEST_PARALLEL_LEVEL} tests in parallel in $ENV{TEST_TMPDIR}")
endif()

# Use a timeout of 10 minutes per test by default
if(DEFINED ENV{TEST_TIMEOUT})
    set(test_timeout "$ENV{TEST_TIMEOUT}")
else()
    set(test_timeout 600)
endif()

# Run all tests, and show test output on failure
execute_process(COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure --timeout ${test_timeout})
