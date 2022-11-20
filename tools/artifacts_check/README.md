# Speedb Artifacts Checker

## Motivation

As part of our release process, we need to test the .a and .so artifacts. our QA tools (unit, stress, and fuzz tests) are all testing the source code and compiling it to be tested. Those tools are unable to test either static or dynamic artifacts.
We would like to create primary testing tools, able to import .a / .so artifact, verify compilation, and no corruption.

## Overview

Sanity check for .a / .so artifact.

## Usage
make check_shared / make check_static while the artifact is in speedb folder. 

