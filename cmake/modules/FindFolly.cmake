find_path(FOLLY_ROOT_DIR
    NAMES include/folly/folly-config.h
)

find_library(FOLLY_LIBRARIES
    NAMES folly
    HINTS ${FOLLY_ROOT_DIR}/lib
)

find_library(FOLLY_BENCHMARK_LIBRARIES
    NAMES follybenchmark
    HINTS ${FOLLY_ROOT_DIR}/lib
)

find_path(FOLLY_INCLUDE_DIR
    NAMES folly/folly-config.h
    HINTS ${FOLLY_ROOT_DIR}/include
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Folly DEFAULT_MSG
    FOLLY_LIBRARIES
    FOLLY_INCLUDE_DIR
)

mark_as_advanced(
    FOLLY_ROOT_DIR
    FOLLY_LIBRARIES
    FOLLY_BENCHMARK_LIBRARIES
    FOLLY_INCLUDE_DIR
)