FROM docker.io/centos:7.9.2009

# Install required libraries and utilities for building Speedb
RUN yum install -y centos-release-scl epel-release
RUN yum install -y make devtoolset-9-gcc-c++ \
	coreutils wget which git python3 openssl-devel \
	libzstd-devel lz4-devel snappy-devel zlib-devel \
	java-1.8.0-openjdk-devel
ENV PATH="/opt/rh/devtoolset-9/root/usr/bin:${PATH}"

# Install CMake
RUN wget https://github.com/Kitware/CMake/releases/download/v3.20.1/cmake-3.20.1.tar.gz
RUN tar xf cmake-3.20.1.tar.gz
RUN cd cmake-3.20.1 && ./bootstrap && make -j $(nproc) && make install && cd .. && rm -rf cmake-3.20.1*

RUN echo -e "#!/bin/sh\n\
set -xeo pipefail\n\
java -version\n\
cd speedb\n\
rm -rf build && mkdir build && cd build\n\
cmake .. -DCMAKE_BUILD_TYPE=Release -DPORTABLE=1 -DWITH_GFLAGS=0 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZLIB=1 -DWITH_ZSTD=1\n\
make -j$(nproc) speedb speedb-shared\n\
cp libspeedb.{a,so} /out\n\
cd .. && rm -rf build\n\
make clean && LIB_MODE=static DEBUG_LEVEL=0 PORTABLE=1 JAVA_HOME=/usr/lib/jvm/java-openjdk make -j$(nproc) rocksdbjavastatic\n\
cp java/target/speedbjni-*.jar /out" > /usr/bin/build-speedb.sh

RUN chmod +x /usr/bin/build-speedb.sh
ENTRYPOINT ["/usr/bin/build-speedb.sh"]

# Declare volumes
VOLUME ["/speedb", "/out"]
