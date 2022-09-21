#!/bin/sh
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# Check for some simple mistakes that should prevent commit or push

BAD=""

if git grep 'namespace rocksdb' -- '*.[ch]*'; then
  echo "^^^^^ Do not hardcode namespace rocksdb. Use ROCKSDB_NAMESPACE"
  BAD=1
fi

if git grep -i 'nocommit' -- ':!build_tools/check-sources.sh'; then
  echo "^^^^^ Code was not intended to be committed"
  BAD=1
fi

if git grep '<rocksdb/' -- ':!build_tools/check-sources.sh'; then
  echo '^^^^^ Use double-quotes as in #include "rocksdb/something.h"'
  BAD=1
fi

if git grep 'using namespace' -- ':!build_tools' ':!docs' \
    ':!third-party/folly/folly/lang/Align.h' \
    ':!third-party/gtest-1.8.1/fused-src/gtest/gtest.h'
then
  echo '^^^^ Do not use "using namespace"'
  BAD=1
fi

if [ "$BAD" ]; then
  exit 1
fi
