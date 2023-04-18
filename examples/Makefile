include ../make_config.mk

PROJECT_NAME?=speedb
LIBNAME?=lib$(PROJECT_NAME)

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

CFLAGS += -Wstrict-prototypes

.PHONY: clean static_lib

all: simple_example column_families_example compaction_filter_example compact_files_example c_simple_example optimistic_transaction_example \
	 transaction_example options_file_example rocksdb_backup_restore_example speedb_is_awesome_example speedb_with_ttl_example

simple_example: static_lib simple_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

column_families_example: static_lib column_families_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compaction_filter_example: static_lib compaction_filter_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compact_files_example: static_lib compact_files_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@ -I../include

c_simple_example: static_lib c_simple_example.o
	$(CXX) $@.o -o$@ ../$(LIBNAME).a $(PLATFORM_LDFLAGS) $(EXEC_LDFLAGS)

optimistic_transaction_example: static_lib optimistic_transaction_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

transaction_example: static_lib transaction_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

options_file_example: static_lib options_file_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

multi_processes_example: static_lib multi_processes_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

speedb_is_awesome_example: static_lib speedb_is_awesome_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

speedb_with_ttl_example: static_lib speedb_with_ttl_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

rocksdb_backup_restore_example: static_lib rocksdb_backup_restore_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../$(LIBNAME).a -I../include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./simple_example ./column_families_example ./compact_files_example ./compaction_filter_example ./c_simple_example c_simple_example.o \
		   ./optimistic_transaction_example ./transaction_example ./options_file_example ./multi_processes_example ./rocksdb_backup_restore_example \
		   ./speedb_is_awesome_example ./speedb_with_ttl_example

static_lib:
	LIBNAME="$(LIBNAME)" $(MAKE) -C .. static_lib
