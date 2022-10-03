# Contributing

<!--
Part of the Speedb project, under the Apache License v2.0.
See /LICENSE for license information.
SPDX-License-Identifier: Apache-2.0
-->

<!-- toc -->

## Table of contents

-   [Overview](#overview)
-   [Ways to contribute](#ways-to-contribute)
    -   [Help document Speedb](#help-document-speedb)
    -   [Help address bugs](#help-address-bugs)
    -   [Help contribute ideas](#help-contribute-ideas)
    -   [Help land changes](#help-land-changes)
-   [How to become a contributor](#how-to-become-a-contributor)
    -   [Contribution guidelines and standards](#contribution-guidelines-and-standards)
-   [Style](#style)
    -   [Source code](#source-code)
    -   [Markdown files](#markdown-files)
-   [License](#license)
    -   [Source files](#source-files-1)
    -   [Markdown](#markdown)
-   [Contribution workflow](#contribution-workflow)
    -   [Fork and build](#fork-and-build)
    -   [Checkout a pull requuest](#checkout-a-pull-request)
    -   [Make your changes](#make-your-changes)
        -   [Update HISTORY.md](#update-HISTORYmd)
        -   [Add a test](#add-a-test)
    -   [Run the tests](#run-the-tests)
        -   [C++ unit tests](#c-unit-tests)
        -   [Debugging single unit test failures](#debugging-single-unit-test-failures)
        -   [Java unit tests](#java-unit-tests)
        -   [Additional build flavors](#additional-build-flavors)
        -   [Crash tests](#crash-tests)
        -   [Performance tests](#performance-tests)
    -   [Commit changes](#commit-changes)
    -   [Create a pull request](#create-a-pull-request)
        -   [Submit a pull request](#submit-a-pull-request)

<!-- tocstop -->

## Overview

Thank you for your interest in contributing to Speedb! There are many ways to
contribute, and we appreciate all of them. If you have questions, please feel
free to ask on [GitHub](https://github.com/speedb-io/speedb/discussions).

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md) to keep our
community welcoming, helpful, and respectable.

## Ways to contribute

There are several ways to contribure to Speedb, the most obvious of which is by
contributing code changes, but it's not the only one.

### Help document Speedb

We strive to provide an extensive and up to date documentation of Speedb, so if
you find an area where the documentation is lacking, we would love to have you
contribute changes to address that.

### Help address bugs

We'll inevitably have bugs, or other kinds of issues. Helping us by reporting
such issues with detailed information (ideally with a test case attached), or
even simply analyzing and reproducing an existing issue, is a great way to get
involved. We track bugs and other kinds of issues using
[GitHub issues](https://github.com/speedb-io/speedb/issues).

Please go over existing issues before opening a new one to avoid duplicates, and
please follow the relevant template when opening new issues.

### Help contribute ideas

If you have an idea for Speedb, we encourage you to
[discuss](https://github.com/speedb-io/speedb/discussions) it with the
community, and potentially prepare a proposal for it and submit it as a feature
request using the
[feature request template](https://github.com/speedb-io/speedb/issues/new?assignees=&labels=&template=feature_request.md&title=).

If you do start working on a proposal, keep in mind that this requires a time
investment to discuss the idea with the community, get it reviewed, and
eventually implemented. We encourage discussing the idea early, before even
writing a proposal.

### Help land changes

If you find a feature request that you'd like to get into Speedb and there's a
pull request open for it, you can help by testing it and providing feedback.
When giving feedback, please keep comments positive and constructive.

## How to become a contributor

### Contribution guidelines and standards

All documents and pull requests must be consistent with the guidelines and
follow the Speedb documentation and coding styles.

-   For **both** documentation and code:

    -   When the Speedb team accepts new documentation or features, we take on
        the maintenance burden. This means we'll weigh the benefit of each
        contribution against the cost of maintaining it.
    -   The appropriate [style](#style) is applied.
    -   The [license](#license) is present in all contributions.
    -   Code review is used to improve the correctness, clarity, and consistency
        of all contributions.

-   For documentation:

    -   All documentation is written for clarity and readability. Beyond fixing
        spelling and grammar, this also means content is worded to be accessible
        to a broad audience.
    -   Typos or other minor fixes that don't change the meaning of a document
        do not need formal review, and are often handled directly as a pull
        request.

-   For code:

    -   New features and substantive changes to Speedb need to go through a
        formal feature request process. Pull requests are only sent after a
        proposal has been discussed, submitted, and reviewed.
    -   Bug fixes and mechanical improvements don't need this.
    -   All new features and bug fixes include unit tests, as they help to (a)
        document and validate concrete usage of a feature and its edge cases,
        and (b) guard against future breaking changes to lower the maintenance
        cost.
    -   Unit tests must pass with the changes.
    -   If some tests fail for unrelated reasons, we wait until they're fixed.
        It helps to contribute a fix!
    -   Code changes should be made with API compatibility and evolvability in
        mind.

## Style

### Source code

Speedb follows the
[Google C++ Style](https://google.github.io/styleguide/cppguide.html).

For formatting, we limit each line to 80 characters. Most formatting can be done
automatically by running

```
build_tools/format-diff.sh
```

or simply `make format` if you use GNU make. If you lack any of the dependencies
to run it, the script will print out instructions for you to install them.

### Markdown files

Markdown files should use [Prettier](https://prettier.io/) for formatting.

## License

A license is required at the top of all documents and files.

### Source files

Every new source file should have the following header at the top:

```
Copyright (C) <year> Speedb Ltd. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

Replace `<year>` in the copyright notice above with the current year.

### Markdown

Markdown files should have at the top:

```
# DOC TITLE

<!--
Part of the Speedb project, under the Apache License v2.0.
See /LICENSE for license information.
SPDX-License-Identifier: Apache-2.0
-->
```

For example, see the top of
[this file](https://github.com/speedb-io/speedb/raw/main/CONTRIBUTING.md)'s raw
content.

## Contribution workflow

As most open-source projects in github, Speedb contributors work on their fork,
and send pull requests to Speedb’s repo. After a reviewer approves the pull
request, a Speedb team member will merge it.

### Fork and build

[Fork](https://github.com/speedb-io/speedb/fork) the Speedb repository to your
own account and clone the resulting repository to your machine.

Refer to the [README](README.md) and [INSTALL](INSTALL.md) documents for
information about how to build Speedb locally.

### Checkout a pull request

If you'd like to contribute by testing a pull request and providing feedback,
this section is for you. Otherwise, if you'd like to contribute by making
changes (to code or documentation), skip this section and read the next one
instead.

Every pull request has its own number. This number is visible both in the URL
of a pull request page as well as in the title of the pull request page itself
(in the form #123, where 123 is the PR number). Follow
[this guide](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/checking-out-pull-requests-locally)
in order to checkout the pull request locally (if you're using GitHub CLI, be
sure to choose the GitHub CLI option rather than Web Browser on the guide page).
After you have the pull request changes checked out locally, you can move on to
testing the changes by using the information in the "Run the tests" section
below.

### Make your changes

This is where you update the documentation, fix a bug, test another
contributor's fix, or add a feature. Make sure your changes adhere to the
guidelines.

If you add a new source file, be sure to add it to the `LIB_SOURCES` variable in
[`src.mk`](src.mk) (note the backslashes at the end of each line) as well as to
the `SOURCES` variable in [`CMakeLists.txt`](CMakeLists.txt).

#### Update HISTORY.md

For code-related changes, add a short description of your change to the
[HISTORY](HISTORY.md) document, especially if it's a bug fix, public API change
or an awesome new feature.

#### Add a test

If you make a code-related change, be sure to add a unit test. Speedb uses
[GTest](https://github.com/google/googletest) for the C++ unit tests and
[JUnit](https://junit.org/) for the Java unit tests.

For the C++ unit test, prefer adding a test to an existing unit tests suite (in
the files ending with `_test.cc`) in order to keep build and test time at bay.
However, if this is a test for a new feature and it doesn't belong in any of the
existing test suites, you may add a new file. Be sure to update the
`TEST_MAIN_SOURCES` variable in [`src.mk`](src.mk) (note the backslashes at the
end of each line) as well as the `TESTS` variable in
[`CMakeLists.txt`](CMakeLists.txt).

### Run the tests

This is only needed for code-related changes, so if you only made changes to
documentation you can safely skip this section.

#### C++ unit tests

You can run the C++ unit tests using the Makefile as explained below, or, if
you're using CMake, using `ctest`. The Makefile has support for running the unit
tests in parallel using GNU Parallel, so it's recommended that you install it
first using your system's package manager (refer to the GNU Parallel
[official webpage](https://www.gnu.org/software/parallel/) for more
information).

In order to run unit tests execute the following command:

```
make check
```

This will build Speedb and run the tests. You can provide the `-j` flag to
`make` in order to make a better utilization of CPU and speed up the build. Note
that this flag only affects the build, not the tests themselves. If you have GNU
Parallel installed, you can control the number parallel tests to run using the
environment variable `J`. For example, to build on a 64-core CPU and run the
tests in parallel, you can run:

```
make J=64 check -j64
```

Unlike `-j`, which if not provided defaults to 1, if `J` isn't provided, the
default is to run one job per core.

If you switch between release and debug build, normal or lite build, or compiler
or compiler options, call `make clean` first. So here is a safe routine to run
all tests:

```
make clean && make check -j64
```

#### Debugging single unit test failures

You can run a specific unit test by running the test binary that contains it. If
you use GNU make, the test binary will be located in the root directory of the
repository (if you use CMake, the test binary will be in your build directory).
For example, the test `DBBasicTest.OpenWhenOpen` is in the binary
`db_basic_test`, so simply running

```
./db_basic_test
```

will run all tests in the binary.

GTest provides some useful command line parameters, and you can see them by
providing the `--help` argument to the test binary:

```
./db_basic_test --help
```

The flag you're most likely to use is probably `--gtest_filter`, which allows
you to specify a subset of the tests to run. For example, if you only want to
run `DBBasicTest.OpenWhenOpen`:

```
./db_basic_test --gtest_filter="*DBBasicTest.OpenWhenOpen*"
```

By default, the test DB created by tests is cleared up even if the test fails.
You can preserve it by using `--gtest_throw_on_failure`. If you want to stop the
debugger when an assertion fails, specify `--gtest_break_on_failure`.

The `KEEP_DB=1` environment variable is another way to preserve the test DB from
being deleted at the end of a unit-test run, regardless of whether the test
fails or not:

```
KEEP_DB=1 ./db_basic_test --gtest_filter=DBBasicTest.Open
```

By default, the temporary test files will be under `/tmp/rocksdbtest-<number>/`
(except when running in parallel, in which case they are under `/dev/shm`). You
can override the location by using the `TEST_TMPDIR` environment variable. For
example:

```
TEST_TMPDIR=/dev/shm/my_dir ./db_basic_test
```

#### Java unit tests

To run the Java unit tests, make sure you set the `JAVA_HOME` environment
variable to the path of your JDK installation and execute the following command:

```
make jclean && DISABLE_JEMALLOC=1 make jtest -j64
```

#### Additional build flavors

For more complicated code changes, we ask contributors to run more build flavors
before sending the code for review.

To build with _AddressSanitizer (ASAN)_, set the `COMPILE_WITH_ASAN` environment
variable:

```
COMPILE_WITH_ASAN=1 make check -j64
```

To build with _ThreadSanitizer (TSAN)_, set the `COMPILE_WITH_TSAN` environment
variable:

```
COMPILE_WITH_TSAN=1 make check -j64
```

To run _UndefinedBehaviorSanitizer (UBSAN)_, set the `COMPILE_WITH_UBSAN`
environment variable:

```
COMPILE_WITH_UBSAN=1 make check -j64
```

To run LLVM's analyzer, run:

```
make analyze
```

#### Crash tests

For changes with higher risks, other than running all of the tests with multiple
flavors, a crash test cycle needs to be executed without failure. If crash test
doesn't cover the new feature, add it there.

To run all crash tests, run

```
make crash_test -j64
make crash_test_with_atomic_flush -j64
```

If you are unable to use GNU make, you can manually build the `db_stress`
binary, and run the following commands manually:

```
  python -u tools/db_crashtest.py whitebox
  python -u tools/db_crashtest.py blackbox
  python -u tools/db_crashtest.py --simple whitebox
  python -u tools/db_crashtest.py --simple blackbox
  python -u tools/db_crashtest.py --cf_consistency blackbox
  python -u tools/db_crashtest.py --cf_consistency whitebox
```

#### Performance tests

For changes that might impact performance, we suggest normal benchmarks are run
to make sure there is no regression (see [benchmark.sh](tools/benchmark.sh)).
Depending the actual performance, you may choose to run against a database
backed by disks, or memory-backed file systems.

### Commit changes

Please keep your commits:

-   Standalone - The code must compile and run successfully after each commit
    (no breaking commits!).
-   Minimal - Break your code into minimal, logically-complete chunks.
-   Self-Reviewed - Always double-check yourself before submitting.

Commit messages should:

-   Start with a component name followed by a colon. For example, if you made
    changes to the documentation, prefix the commit message with `docs: `. If
    you only updated tests, prefix the commit message with `tests: `. For
    build-related changed use `build: `, etc.
-   Reference a relevant issue, if any. This is especially relevant for bug
    fixes and new features. The issue should be referenced at the end of the
    first line as a hash sign followed by the issue number. For example, `#23`.
    If there's more than one issue that applies, mention the main one on the
    first line, and add a reference to the rest at the end of the commit message
    (e.g. `Also fixes #54, #89, and #99`).
-   Have the line length limited to 100 characters or less. This restriction
    does not apply when quoting program output, etc.
-   Be phrased in a clear and grammatically-correct language, and use present
    tense ("add feature", not "added feature".)

### Create a pull request

When you're finished with the changes, create a pull request, also known as a
PR. If you're unfamiliar with open-source contributions on GitHub, follow the
[Creating a pull request guide](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).

#### Submit a pull request

-   Describe what your change is doing, especially if there isn't a relevant
    issue open.
-   Reference relevant issues and discussions, and don't forget to
    [link PR to issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue)
    if you are solving one.
-   Explain how you tested your changes (we recommend adding a "Test Plan:"
    section to the pull request summary, which specifies what testing was done
    to validate the quality and performance of the change).
-   If your change impacts performance, explain why the specific performance
    environment was chosen. Also specify at least one benchmark test case that
    favors the improvement and share the results.
-   Enable the checkbox to allow maintainer edits so the branch can be updated
    for a merge. Once you submit your PR, a Speedb team member will review your
    proposal. We may ask questions or request for additional information.
-   We may ask for changes to be made before a PR can be merged, either using
    suggested changes or pull request comments. You can apply suggested changes
    directly through the UI. You can make any other changes in your fork, then
    commit them to your branch.
-   If you run into any merge issues, check out this
    [git tutorial](https://lab.github.com/githubtraining/managing-merge-conflicts)
    to help you resolve merge conflicts and other issues.
