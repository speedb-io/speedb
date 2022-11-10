set(PLUGINS_LIB "${PROJECT_NAME}_plugins")
add_library(${PLUGINS_LIB} OBJECT)

if (ROCKSDB_PLUGINS)
  separate_arguments(ROCKSDB_PLUGINS)
endif()
if (NOT ROCKSDB_PLUGINS OR NOT "speedb" IN_LIST ROCKSDB_PLUGINS)
  list(APPEND ROCKSDB_PLUGINS speedb)
endif()

set(ROCKSDB_PLUGIN_EXTERNS "")
set(ROCKSDB_PLUGIN_BUILTINS "")

message(STATUS "ROCKSDB_PLUGINS: ${ROCKSDB_PLUGINS}")

if(ROCKSDB_PLUGINS)
  foreach (plugin ${ROCKSDB_PLUGINS})
    add_subdirectory(${plugin})

    # Use get_directory_property() to avoid having to declare the variables
    # with PARENT_SCOPE in the plugin CMakeLists.txt
    # TODO: Change the plugin support here so that a plugin would simply define
    # a target that we'll link to.
    get_directory_property(${plugin}_SOURCES
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_SOURCES)
    get_directory_property(${plugin}_COMPILE_FLAGS
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_COMPILE_FLAGS)
    foreach (src ${${plugin}_SOURCES})
      target_sources(${PLUGINS_LIB} PRIVATE "${plugin}/${src}")
      set_source_files_properties(
        "${plugin}/${src}"
        PROPERTIES COMPILE_FLAGS "${${plugin}_COMPILE_FLAGS}")
    endforeach()

    get_directory_property(${plugin}_TESTS
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_TESTS)
    foreach (test ${${plugin}_TESTS})
      list(APPEND PLUGIN_TESTS ${plugin}/${test})
      set_source_files_properties(
        ${plugin}/${test}
        PROPERTIES COMPILE_FLAGS "${${plugin}_COMPILE_FLAGS}")
    endforeach()

    get_directory_property(${plugin}_INCLUDE_PATHS
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_INCLUDE_PATHS)
    target_include_directories(${PLUGINS_LIB} PRIVATE ${plugin}_INCLUDE_PATHS)

    get_directory_property(${plugin}_LIBS
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_LIBS)
    target_link_libraries(${PLUGINS_LIB} INTERFACE ${${plugin}_LIBS})

    get_directory_property(${plugin}_LINK_PATHS
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_LINK_PATHS)
    target_link_directories(${PLUGINS_LIB} INTERFACE ${${plugin}_LINK_PATHS})

    get_directory_property(${plugin}_CMAKE_SHARED_LINKER_FLAGS
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_CMAKE_SHARED_LINKER_FLAGS)
    get_directory_property(${plugin}_CMAKE_EXE_LINKER_FLAGS
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_CMAKE_EXE_LINKER_FLAGS)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${${plugin}_CMAKE_SHARED_LINKER_FLAGS}" PARENT_SCOPE)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${${plugin}_CMAKE_EXE_LINKER_FLAGS}" PARENT_SCOPE)

    # Add plugin registration function, if set
    get_directory_property(${plugin}_FUNC
      DIRECTORY ${plugin}
      DEFINITION ${plugin}_FUNC)
    string(STRIP "${${plugin}_FUNC}" ${plugin}_FUNC)
    if (NOT "${plugin}_FUNC" STREQUAL "")
      string(APPEND ROCKSDB_PLUGIN_BUILTINS "{\"${plugin}\", ${${plugin}_FUNC} },")
      string(APPEND ROCKSDB_PLUGIN_EXTERNS "int ${${plugin}_FUNC} (ROCKSDB_NAMESPACE::ObjectLibrary&, const std::string&); ")
    endif()
  endforeach()
endif()

set(PLUGIN_REGISTRY_CC ${CMAKE_CURRENT_BINARY_DIR}/plugin_registry.cc)
configure_file(plugin_registry.cc.in ${PLUGIN_REGISTRY_CC} @ONLY)
target_sources(${PLUGINS_LIB} PRIVATE ${PLUGIN_REGISTRY_CC})