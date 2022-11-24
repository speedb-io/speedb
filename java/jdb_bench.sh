# shellcheck disable=SC2148
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
PLATFORM=64
if [ `getconf LONG_BIT` != "64" ]
then
  PLATFORM=32
fi

SPEEDB_JAR=`find target -name speedbjni*.jar`

echo "Running benchmark in $PLATFORM-Bit mode."
# shellcheck disable=SC2068
java -server -d$PLATFORM -XX:NewSize=4m -XX:+AggressiveOpts -Djava.library.path=target -cp "${SPEEDB_JAR}:benchmark/target/classes" org.rocksdb.benchmark.DbBenchmark $@
