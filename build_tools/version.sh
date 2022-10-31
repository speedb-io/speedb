#!/bin/sh
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
if [ "$#" = "0" ]; then
  echo "Usage: $0 major|minor|patch|full"
  exit 1
fi

if [ "$1" = "major" ]; then
  grep MAJOR speedb/version.h | head -n1 | awk '{print $3}'
fi
if [ "$1" = "minor" ]; then
  grep MINOR speedb/version.h | head -n1 | awk '{print $3}'
fi
if [ "$1" = "patch" ]; then
  grep PATCH speedb/version.h | head -n1 | awk '{print $3}'
fi
if [ "$1" = "full" ]; then
  awk '/#define SPEEDB/ { env[$2] = $3 }
       END { printf "%s.%s.%s\n", env["SPEEDB_MAJOR"],
                                  env["SPEEDB_MINOR"],
                                  env["SPEEDB_PATCH"] }'  \
      speedb/version.h
fi
