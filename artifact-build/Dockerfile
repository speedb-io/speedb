FROM docker.io/centos:7.9.2009

# Install required libraries and utilities for building speedb and minio
RUN yum install -y centos-release-scl epel-release
RUN yum install -y devtoolset-7-gcc-c++ lz4-devel snappy-devel sudo zlib-devel wget which git python3 make openssl openssl-devel gflags-devel libzstd-devel libzstd java-1.8.0-openjdk java-1.8.0-openjdk-devel coreutils
ENV PATH="/opt/rh/devtoolset-7/root/usr/bin:${PATH}"

# Install CMake
RUN wget https://github.com/Kitware/CMake/releases/download/v3.20.1/cmake-3.20.1.tar.gz
RUN tar xf cmake-3.20.1.tar.gz
RUN cd cmake-3.20.1 && ./bootstrap && make -j $(nproc) && make install && cd .. && rm -rf cmake-3.20.1*

RUN echo -e "#!/bin/sh\n\
java -version\n\
cd speedb && \\\\\n\
export JAVA_HOME=$(dirname $(dirname $(readlink $(readlink $(which javac)))))\n\
export PATH=$PATH:$JAVA_HOME/bin\n\
export CLASSPATH=.:$JAVA_HOME/jre/lib:$JAVA_HOME/lib:$JAVA_HOME/lib/tools.jar\n\
ls -alFh\n\
make clean && \\\\\n\
make DEBUG_LEVEL=0 PORTABLE=1 -j16 -j$(nproc) rocksdbjavastatic\n\
ls -alFh java/target\n\
find java/target/ -iregex '.*\.jar' -exec cp {} /out \; || echo regex failed\n\
make clean && \\\\\n\
LIB_MODE=shared DEBUG_LEVEL=0 PORTABLE=1 make -j16 -j$(nproc) shared_lib DEBUG_LEVEL=0 PORTABLE=1 shared_lib\n\
ls -alFh .\n\
cp ./librocksdb.so /out || echo regex failed\n\
make clean && \\\\\n\
DEBUG_LEVEL=0 PORTABLE=1 make -j$(nproc) static_lib\n\
ls -alFh .\n\
cp ./*librocksdb.* /out || echo regex failed\n\
ls -alFh /out"> /usr/bin/build-speedb.sh 

RUN chmod +x /usr/bin/build-speedb.sh
ENTRYPOINT ["/usr/bin/build-speedb.sh"]

# Declare output volume
VOLUME ["/out"]
