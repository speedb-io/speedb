# Read Speedb version from version.h header file.

function(get_speedb_version version_var)
  file(READ "${CMAKE_CURRENT_SOURCE_DIR}/speedb/version.h" version_header_file)
  foreach(component MAJOR MINOR PATCH)
    string(REGEX MATCH "#define SPEEDB_${component} ([0-9]+)" _ ${version_header_file})
    set(SPEEDB_VERSION_${component} ${CMAKE_MATCH_1})
  endforeach()
  set(${version_var} "${SPEEDB_VERSION_MAJOR}.${SPEEDB_VERSION_MINOR}.${SPEEDB_VERSION_PATCH}" PARENT_SCOPE)
endfunction()
