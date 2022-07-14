# Ways to Contribute

There are several ways to contribure to Speedb, other than contributing code changes:

* Add or update document.
* Report issue with detailed information, if possible attache a test.
* Investigate and fix an issue.
* Review and test a PR. 
* Submit new feature request or implementation. We welcome any feature suggestions, for new feature implementation, please create an issue with the design proposal first. It can help to get more engagement and discussion before reviewing your implementation;


# Basic Development Workflow

As most open-source projects in github, Speedb contributors work on their fork, and send pull requests to Speedb’s repo. After a reviewer approves the pull request, a Speedb team member will merge it.

# How to Build

Refer to the [INSTALL](INSTALL.md) document for information about how to build Speedb.

# How to Run Unit Tests

## Build Systems

The makefile used for _GNU make_ has some supports to help developers run all unit tests in parallel, which will be introduced below. If you use CMake, you can run the tests with `ctest`.

## Running the Unit Tests in Parallel

In order to run unit tests in parallel, first install _GNU parallel_ on your host, and run
```
make check
```
This will build Speedb and run the tests. You can provide the `-j` flag to `make` in order to make a better utilization of CPU and speed up the build.

You can specify number of parallel tests to run using environment variable `J`, for example:
```
make J=64 check -j64
```

If `J` isn't provided, the default is to run one job per core.

If you switch between release and debug build, normal or lite build, or compiler or compiler options, call `make clean` first. So here is a safe routine to run all tests:

```
make clean && make check -j64
```

## Debugging Single Unit Test Failures

Speedb uses _GTest_. You can run specific unit test by running the test binary that contains it. If you use GNU make, the test binary will be in the root directory of the repository (if you use CMake, the test binary will be in your build directory). For example, test `DBBasicTest.OpenWhenOpen` is in binary `db_basic_test`, so simply running
```
./db_basic_test
```
will run all tests in the binary.

GTest provides some useful command line parameters, and you can see them by calling `--help`:
```
./db_basic_test --help
```
Here are some frequently used ones:

Running a subset of tests is possible using `--gtest_filter`. For example, if you only want to run `DBBasicTest.OpenWhenOpen`, call
```
./db_basic_test --gtest_filter=“*DBBasicTest.OpenWhenOpen*”
```
By default, the test DB created by tests is cleared up even if test fails. You can try to preserve it by using `--gtest_throw_on_failure`. If you want to stop the debugger when assert fails, specify `--gtest_break_on_failure`. `KEEP_DB=1` environment variable is another way to preserve the test DB from being deleted at the end of a unit-test run, irrespective of whether the test fails or not:
```
KEEP_DB=1 ./db_basic_test --gtest_filter=DBBasicTest.Open
```

By default, the temporary test files will be under `/tmp/rocksdbtest-<number>/` (except when running in parallel they are under /dev/shm). You can override the location by using environment variable `TEST_TMPDIR`. For example:
```
TEST_TMPDIR=/dev/shm/my_dir ./db_basic_test
```
## Java Unit Tests

Sometimes we need to run Java tests too. Run
```
make jclean rocksdbjava jtest
```
Running with `-j` prvided can sometimes cause problems, so try to remove `-j` if you see any errors.

## Additional Build Flavors

For more complicated code changes, we ask contributors to run more build flavors before sending the code for review.

To build with _AddressSanitizer (ASAN)_, set environment variable `WITH_ASAN`:
```
COMPILE_WITH_ASAN=1 make check -j64
```
To build with _ThreadSanitizer (TSAN)_, set environment variable `COMPILE_WITH_TSAN`:
```
COMPILE_WITH_TSAN=1 make check -j64
```
To run all `valgrind tests`:
```
make valgrind_test -j64
```
To run _UndefinedBehaviorSanitizer (UBSAN)_, set environment variable `COMPILE_WITH_UBSAN`:
```
COMPILE_WITH_UBSAN=1 make check -j
```
To run LLVM's analyzer, run
```
make analyze
```

# Code Style

Speedb follows the [Google C++ Style](https://google.github.io/styleguide/cppguide.html).

For formatting, we limit each line to 80 characters. Most formatting can be done automatically by running
```
build_tools/format-diff.sh
```
or simply `make format` if you use _GNU make_. If you lack any of the dependencies to run it, the script will print out instructions for you to install them.

# Requirements Before Sending a Pull Request

## Ensure the Code is Formatted Correctly
As a minimum, run `make format` or `build_tools/format-diff.sh`, as described above.

## HISTORY.md
Consider updating HISTORY.md to mention your change, especially if it's a bug fix, public API change or an awesome new feature.

## Pull Request Summary
Describe what your change is doing, especially if there isn't a relevant issue open, reference relevant issues and discussion, and how you tested your changes (we recommend adding a "Test Plan:" section to the pull request summary, which specifies what testing was done to validate the quality and performance of the change).

## Add Unit Tests
Almost all code changes need to go with changes in unit tests for validation. For new features, new unit tests or tests scenarios need to be added even if it has been validated manually. This is to make sure future contributors can rerun the tests to ensure that their changes don't cause problem with the feature.

## Run the Tests Locally

### For Simple Changes
Pull requests for simple changes can be sent after running all unit tests with any build flavor and see all tests pass. If any public interface is changed, or Java code involved, Java tests also need to be run.

### For Complex Changes
If the change is complicated enough, ASAN, TSAN and valgrind need to be run on your local environment before sending the pull request. If you run ASAN with a recent enough version of LLVM which covers almost all the functionality of valgrind, valgrind tests can be skipped.
Running the valgrind tests may be difficult for developers who use Windows. Just try to use the best equivalent tools available in your environment.

### Changes with Higher Risk or Some Unknowns
For changes with higher risks, other than running all of the tests with multiple flavors, a crash test cycle (see [[Stress Test]]) needs to be executed without failure. If crash test doesn't cover the new feature, add it there.

To run all crash tests, run
```
make crash_test -j64
make crash_test_with_atomic_flush -j64
```

If you are unable to use _GNU make_, you can manually build the `db_stress` binary, and run the following commands manually:
```
  python -u tools/db_crashtest.py whitebox
  python -u tools/db_crashtest.py blackbox
  python -u tools/db_crashtest.py --simple whitebox
  python -u tools/db_crashtest.py --simple blackbox
  python -u tools/db_crashtest.py --cf_consistency blackbox
  python -u tools/db_crashtest.py --cf_consistency whitebox 
```

### Performance Improvement Changes
For changes that might impact performance, we suggest normal benchmarks are run to make sure there is no regression. Depending the actual performance, you may choose to run against a database backed by disks, or memory-backed file systems. Explain in the pull request summary why the specific performance environment was chosen. If the change is to improve performance, bring at least one benchmark test case that favors the improvement and share the results.
