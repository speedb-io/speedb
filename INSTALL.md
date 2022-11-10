## Compilation

**Important**: If you plan to run Speedb in production, don't compile using
default `make` or `make all` invocations. That will compile Speedb in debug
mode, which is much slower than release mode.

Speedb's library should be able to compile without any dependency installed,
although we recommend installing some compression libraries (see below). We do
depend on newer gcc/clang with C++17 support (GCC >= 7, Clang >= 5).

There are few options when compiling Speedb:

-   [recommended] `make static_lib` will compile the Speedb static library
    (`libspeedb.a`) in release mode.

-   `make shared_lib` will compile the Speedb shared library (`libspeedb.so`)
    in release mode.

-   `make check` will compile and run all the unit tests. `make check` will
    compile Speedb in debug mode.

-   `make all` will compile our static library, and all our tools and unit
    tests. Our tools depend on gflags. You will need to have gflags installed to
    run `make all`. This will compile Speedb in debug mode. Don't use binaries
    compiled by `make all` in production.

-   By default the binary we produce is optimized for the platform you're
    compiling on (`-march=native` or the equivalent). SSE4.2 will thus be
    enabled automatically if your CPU supports it. To print a warning if your
    CPU does not support SSE4.2, build with `USE_SSE=1 make static_lib` or, if
    using CMake, `cmake -DFORCE_SSE42=ON`. If you want to build a portable
    binary, add `PORTABLE=1` before your make commands, like this:
    `PORTABLE=1 make static_lib`, or `cmake -DPORTABLE=1` if using CMake.

## Dependencies

-   You can link Speedb with following compression libraries:

    -   [zlib](http://www.zlib.net/) - a library for data compression.
    -   [bzip2](http://www.bzip.org/) - a library for data compression.
    -   [lz4](https://github.com/lz4/lz4) - a library for extremely fast data
        compression.
    -   [snappy](http://google.github.io/snappy/) - a library for fast data
        compression.
    -   [zstandard](http://www.zstd.net) - Fast real-time compression algorithm.

-   All of our tools depend on:

    -   [gflags](https://gflags.github.io/gflags/) - a library that handles
        command line flags processing. Note that this only required for building
        the tools, and that you can compile the Speedb library even if you don't
        have gflags installed.

-   `make check` will also check code formatting, which requires
    [clang-format](https://clang.llvm.org/docs/ClangFormat.html)

-   If you wish to build the RocksJava static target, then CMake is required for
    building Snappy.

-   If you wish to run microbench (e.g, `make microbench`, `make ribbon_bench`
    or `cmake -DWITH_BENCHMARK=1`), Google benchmark >= 1.6.0 is needed.
    -  You can do the following to install Google benchmark. These commands are copied from `./build_tools/ubuntu20_image/Dockerfile`:

    `$ git clone --depth 1 --branch v1.7.0 https://github.com/google/benchmark.git ~/benchmark`

    `$ cd ~/benchmark && mkdir build && cd build && cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_GTEST_TESTS=0 && ninja && ninja install`

## Supported platforms

-   **Linux - Ubuntu**

    -   Upgrade your gcc to version at least 7 to get C++17 support.
    -   Install gflags. First, try: `sudo apt-get install libgflags-dev` If this
        doesn't work and you're using Ubuntu, here's a nice tutorial:
        (http://askubuntu.com/questions/312173/installing-gflags-12-04)
    -   Install snappy. This is usually as easy as:
        `sudo apt-get install libsnappy-dev`.
    -   Install zlib. Try: `sudo apt-get install zlib1g-dev`.
    -   Install bzip2: `sudo apt-get install libbz2-dev`.
    -   Install lz4: `sudo apt-get install liblz4-dev`.
    -   Install zstandard: `sudo apt-get install libzstd-dev`.

-   **Linux - CentOS / RHEL**

    -   Upgrade your gcc to version at least 7 to get C++17 support
    -   Install gflags:

                git clone https://github.com/gflags/gflags.git
                cd gflags
                git checkout v2.0
                ./configure && make && sudo make install

        **Notice**: Once installed, please add the include path for gflags to
        your `CPATH` environment variable and the lib path to `LIBRARY_PATH`. If
        installed with default settings, the include path will be
        `/usr/local/include` and the lib path will be `/usr/local/lib`.

    -   Install snappy:

                sudo yum install snappy snappy-devel

    -   Install zlib:

                sudo yum install zlib zlib-devel

    -   Install bzip2:

                sudo yum install bzip2 bzip2-devel

    -   Install lz4:

                sudo yum install lz4-devel

    -   Install ASAN (optional for debugging):

                sudo yum install libasan

    -   Install zstandard:

        -   With [EPEL](https://fedoraproject.org/wiki/EPEL):

                sudo yum install libzstd-devel

        -   With CentOS 8:

                sudo dnf install libzstd-devel
* **iOS**:
  * Run: `TARGET_OS=IOS make static_lib`. When building the project which uses rocksdb iOS library, make sure to define an important pre-processing macros: `IOS_CROSS_COMPILE`.

        -   From source:

                wget https://github.com/facebook/zstd/archive/v1.1.3.tar.gz
                mv v1.1.3.tar.gz zstd-1.1.3.tar.gz
                tar zxvf zstd-1.1.3.tar.gz
                cd zstd-1.1.3
                make && sudo make install

-   **OS X**:

    -   Install latest C++ compiler that supports C++ 17:
        -   Update XCode: run `xcode-select --install` (or install it from XCode
            App's settting).
        -   Install via [homebrew](http://brew.sh/).
            -   If you're first time developer in MacOS, you still need to run:
                `xcode-select --install` in your command line.
            -   run `brew tap homebrew/versions; brew install gcc7 --use-llvm`
                to install gcc 7 (or higher).

-   **Windows** (Visual Studio 2017 to up):
    -   Read and follow the instructions at CMakeLists.txt
