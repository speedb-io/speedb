ifndef PYTHON

# Default to python3. Some distros like CentOS 8 do not have `python`.
ifeq ($(origin PYTHON), undefined)
	PYTHON := $(shell which python3 || which python || echo python3)
endif
export PYTHON

endif

# To setup tmp directory, first recognize some old variables for setting
# test tmp directory or base tmp directory. TEST_TMPDIR is usually read
# by RocksDB tools though Env/FileSystem::GetTestDirectory.
ifeq ($(TEST_TMPDIR),)
TEST_TMPDIR := $(TMPD)
endif
# Avoid setting up the tmp directory on Makefile restarts or when the target
# isn't a check target
ifeq ($(MAKE_RESTARTS),)
ifneq ($(filter %check,$(MAKECMDGOALS)),)

ifeq ($(TEST_TMPDIR),)
ifeq ($(BASE_TMPDIR),)
BASE_TMPDIR :=$(TMPDIR)
endif
ifeq ($(BASE_TMPDIR),)
BASE_TMPDIR :=/tmp
endif
# Use /dev/shm on Linux if it has the sticky bit set (otherwise, /tmp or other
# base dir), and create a randomly-named rocksdb.XXXX directory therein.
ifneq ($(shell [ "$$(uname -s)" = "Linux" ] && [ -k /dev/shm ] && echo 1),)
BASE_TMPDIR :=/dev/shm
endif
TEST_TMPDIR := $(shell mktemp -d "$(BASE_TMPDIR)/rocksdb.XXXX")
endif

# The `export` line below doesn't work in case Make restarts (due to included
# makefiles getting remade), so we need to output the directory we created into
# a temporary config file that will be included by the `include` directive below
# in case of a restart (we don't want to output it into make_config.mk in order
# to avoid having the TEST_TMPDIR implicitly set for test that are run through
# makefiles that include make_config.mk, and because we don't want to change
# make_config.mk on every run)
$(shell echo 'TEST_TMPDIR?=$(TEST_TMPDIR)' > test_config.mk)

endif
endif

-include test_config.mk

export TEST_TMPDIR
