G})
  endif()

  if(DEFINED ENV{ZLIB_LIB_RELEASE})
    set(ZLIB_LIB_RELEASE $ENV{ZLIB_LIB_RELEASE})
  endif()
  
  set(ZLIB_CXX_FLAGS -DZLIB)
  set(ZLIB_LIBS debug ${ZLIB_LIB_DEBUG} optimized ${ZLIB_LIB_RELEASE})

  add_definitions(${ZLIB_CXX_FLAGS})
  include_directories(${ZLIB_INCLUDE})
  set (THIRDPARTY_LIBS ${THIRDPARTY_LIBS} ${ZLIB_LIBS})
else ()
  message(STATUS "ZLIB library is disabled")
endif ()

# ================================================== XPRESS ==================================================
# This makes use of built-in Windows API, no additional includes, links to a system lib

# For compatibilty
if (XPRESS)
  set(WITH_XPRESS ON)
endif ()

if (WITH_XPRESS)
  message(STATUS "XPRESS is enabled")

  add_definitions(-DXPRESS)
  
  # We are using the implementation provided by the system
  set (SYSTEM_LIBS ${SYSTEM_LIBS} Cabinet.lib)
else ()
  message(STATUS "XPRESS is disabled")
endif ()


# ================================================== ZSTD ==================================================
#
# Edit these 4 lines to define paths to ZSTD
#
set(ZSTD_HOME $ENV{THIRDPARTY_HOME}/ZSTD.Library)
set(ZSTD_INCLUDE ${ZSTD_HOME}/build/native/inc)
set(ZSTD_LIB_DEBUG ${ZSTD_HOME}/lib/native/debug/amd64/libzstd_static.lib)
set(ZSTD_LIB_RELEASE ${ZSTD_HOME}/lib/native/retail/amd64/libzstd_static.lib)

# For compatibility
if (ZSTD)
  set(WITH_ZSTD ON)
endif ()

if (WITH_ZSTD)
  message(STATUS "ZSTD library is enabled")

  if(DEFINED ENV{ZSTD_INCLUDE})
    set(ZSTD_INCLUDE $ENV{ZSTD_INCLUDE})
  endif()
  
  if(DEFINED ENV{ZSTD_LIB_DEBUG})
    set(ZSTD_LIB_DEBUG $ENV{ZSTD_LIB_DEBUG})
  endif()

  if(DEFINED ENV{ZSTD_LIB_RELEASE})
    set(ZSTD_LIB_RELEASE $ENV{ZSTD_LIB_RELEASE})
  endif()

  # ZSTD_STATIC_LINKING_ONLY only allows us to create an allocation functions override
  # When jemalloc is in use
  set(ZSTD_LIBS debug ${ZSTD_LIB_DEBUG} optimized ${ZSTD_LIB_RELEASE})

  add_definitions(-DZSTD -DZSTD_STATIC_LINKING_ONLY)
  include_directories(${ZSTD_INCLUDE})
  set (THIRDPARTY_LIBS ${THIRDPARTY_LIBS} ${ZSTD_LIBS})
else ()
  message(STATUS "ZSTD library is disabled")
endif ()

#
# Edit these 4 lines to define paths to Jemalloc
#
set(JEMALLOC_HOME $ENV{THIRDPARTY_HOME}/Jemalloc.Library)
set(JEMALLOC_INCLUDE ${JEMALLOC_HOME}/build/native/inc)
set(JEMALLOC_LIB_DEBUG ${JEMALLOC_HOME}/lib/native/debug/amd64/jemalloc.lib)
set(JEMALLOC_LIB_RELEASE ${JEMALLOC_HOME}/lib/native/retail/amd64/jemalloc.lib)

# ================================================== JEMALLOC ==================================================
if(JEMALLOC)
  set(WITH_JEMALLOC ON)
endif()

if (WITH_JEMALLOC)
  message(STATUS "JEMALLOC library is enabled")
  set(JEMALLOC_CXX_FLAGS "-DROCKSDB_JEMALLOC -DJEMALLOC_EXPORT= -DJEMALLOC_NO_RENAME")
  
  if(DEFINED ENV{JEMALLOC_INCLUDE})
    set(JEMALLOC_INCLUDE $ENV{JEMALLOC_INCLUDE})
  endif()
  
  if(DEFINED ENV{JEMALLOC_LIB_DEBUG})
    set(JEMALLOC_LIB_DEBUG $ENV{JEMALLOC_LIB_DEBUG})
  endif()

  if(DEFINED ENV{JEMALLOC_LIB_RELEASE})
    set(JEMALLOC_LIB_RELEASE $ENV{JEMALLOC_LIB_RELEASE})
  endif()

  set(JEMALLOC_LIBS debug ${JEMALLOC_LIB_DEBUG} optimized ${JEMALLOC_LIB_RELEASE})

  add_definitions(${JEMALLOC_CXX_FLAGS})
  include_directories(${JEMALLOC_INCLUDE})
  set (THIRDPARTY_LIBS ${THIRDPARTY_LIBS} ${JEMALLOC_LIBS})
  set (ARTIFACT_SUFFIX "_je")

else ()
  set (ARTIFACT_SUFFIX "")
  message(STATUS "JEMALLOC library is disabled")
endif ()
