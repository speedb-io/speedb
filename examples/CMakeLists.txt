add_executable(simple_example
  simple_example.cc)
target_link_libraries(simple_example
  ${PROJECT_NAME})

add_executable(column_families_example
  column_families_example.cc)
target_link_libraries(column_families_example
  ${PROJECT_NAME})

add_executable(compact_files_example
  compact_files_example.cc)
target_link_libraries(compact_files_example
  ${PROJECT_NAME})

add_executable(c_simple_example
  c_simple_example.c)
target_link_libraries(c_simple_example
  ${PROJECT_NAME})

add_executable(optimistic_transaction_example
  optimistic_transaction_example.cc)
target_link_libraries(optimistic_transaction_example
  ${PROJECT_NAME})

add_executable(transaction_example
  transaction_example.cc)
target_link_libraries(transaction_example
  ${PROJECT_NAME})

add_executable(compaction_filter_example
  compaction_filter_example.cc)
target_link_libraries(compaction_filter_example
  ${PROJECT_NAME})

add_executable(options_file_example
  options_file_example.cc)
target_link_libraries(options_file_example
  ${PROJECT_NAME})

add_executable(multi_processes_example
  EXCLUDE_FROM_ALL
  multi_processes_example.cc)
target_link_libraries(multi_processes_example
  ${PROJECT_NAME})
