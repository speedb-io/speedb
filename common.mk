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
export TEST_TMPDIR
