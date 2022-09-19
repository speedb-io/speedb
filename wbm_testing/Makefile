include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

CFLAGS += -Wstrict-prototypes -g

.PHONY: clean librocksdb

all: multiple_cf stop_write multiple_db


multiple_cf: librocksdb multiple_cf.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@  -g ../librocksdb.a -I../include -O0 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

multiple_db: librocksdb multiple_db.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@  -g ../librocksdb.a -I../include -O0 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

stop_write: librocksdb stop_write.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@  -g ../librocksdb.a -I../include -O0 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./multiple_cf ./ultimate 

librocksdb:
	cd .. && $(MAKE) -j 16 static_lib

librocksdb_debug:
	cd .. && DEBUG_LEVEL=2 $(MAKE) -j 16  debug_lib
