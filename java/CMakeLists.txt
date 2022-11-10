cmake_minimum_required(VERSION 3.12)

set(CMAKE_JAVA_COMPILE_FLAGS -source 8 -Xlint:deprecation -Xlint:unchecked)

set(JNI_NATIVE_SOURCES
  rocksjni/backup_engine_options.cc
  rocksjni/backupenginejni.cc
  rocksjni/cache.cc
  rocksjni/cassandra_compactionfilterjni.cc
  rocksjni/cassandra_value_operator.cc
  rocksjni/checkpoint.cc
  rocksjni/clock_cache.cc
  rocksjni/columnfamilyhandle.cc
  rocksjni/compact_range_options.cc
  rocksjni/compaction_filter.cc
  rocksjni/compaction_filter_factory.cc
  rocksjni/compaction_filter_factory_jnicallback.cc
  rocksjni/compaction_job_info.cc
  rocksjni/compaction_job_stats.cc
  rocksjni/compaction_options.cc
  rocksjni/compaction_options_fifo.cc
  rocksjni/compaction_options_universal.cc
  rocksjni/comparator.cc
  rocksjni/comparatorjnicallback.cc
  rocksjni/compression_options.cc
  rocksjni/concurrent_task_limiter.cc
  rocksjni/config_options.cc
  rocksjni/env.cc
  rocksjni/env_options.cc
  rocksjni/event_listener.cc
  rocksjni/event_listener_jnicallback.cc
  rocksjni/filter.cc
  rocksjni/ingest_external_file_options.cc
  rocksjni/iterator.cc
  rocksjni/jnicallback.cc
  rocksjni/loggerjnicallback.cc
  rocksjni/lru_cache.cc
  rocksjni/memory_util.cc
  rocksjni/memtablejni.cc
  rocksjni/merge_operator.cc
  rocksjni/native_comparator_wrapper_test.cc
  rocksjni/optimistic_transaction_db.cc
  rocksjni/optimistic_transaction_options.cc
  rocksjni/options.cc
  rocksjni/options_util.cc
  rocksjni/persistent_cache.cc
  rocksjni/ratelimiterjni.cc
  rocksjni/remove_emptyvalue_compactionfilterjni.cc
  rocksjni/restorejni.cc
  rocksjni/rocks_callback_object.cc
  rocksjni/rocksdb_exception_test.cc
  rocksjni/rocksjni.cc
  rocksjni/slice.cc
  rocksjni/snapshot.cc
  rocksjni/sst_file_manager.cc
  rocksjni/sst_file_reader_iterator.cc
  rocksjni/sst_file_readerjni.cc
  rocksjni/sst_file_writerjni.cc
  rocksjni/sst_partitioner.cc
  rocksjni/statistics.cc
  rocksjni/statisticsjni.cc
  rocksjni/table.cc
  rocksjni/table_filter.cc
  rocksjni/table_filter_jnicallback.cc
  rocksjni/testable_event_listener.cc
  rocksjni/thread_status.cc
  rocksjni/trace_writer.cc
  rocksjni/trace_writer_jnicallback.cc
  rocksjni/transaction.cc
  rocksjni/transaction_db.cc
  rocksjni/transaction_db_options.cc
  rocksjni/transaction_log.cc
  rocksjni/transaction_notifier.cc
  rocksjni/transaction_notifier_jnicallback.cc
  rocksjni/transaction_options.cc
  rocksjni/ttl.cc
  rocksjni/wal_filter.cc
  rocksjni/wal_filter_jnicallback.cc
  rocksjni/write_batch.cc
  rocksjni/write_batch_test.cc
  rocksjni/write_batch_with_index.cc
  rocksjni/write_buffer_manager.cc
  rocksjni/writebatchhandlerjnicallback.cc
)

set(JAVA_MAIN_CLASSES
  src/main/java/org/rocksdb/AbstractCompactionFilter.java
  src/main/java/org/rocksdb/AbstractCompactionFilterFactory.java
  src/main/java/org/rocksdb/AbstractComparator.java
  src/main/java/org/rocksdb/AbstractComparatorJniBridge.java
  src/main/java/org/rocksdb/AbstractEventListener.java
  src/main/java/org/rocksdb/AbstractImmutableNativeReference.java
  src/main/java/org/rocksdb/AbstractMutableOptions.java
  src/main/java/org/rocksdb/AbstractNativeReference.java
  src/main/java/org/rocksdb/AbstractRocksIterator.java
  src/main/java/org/rocksdb/AbstractSlice.java
  src/main/java/org/rocksdb/AbstractTableFilter.java
  src/main/java/org/rocksdb/AbstractTraceWriter.java
  src/main/java/org/rocksdb/AbstractTransactionNotifier.java
  src/main/java/org/rocksdb/AbstractWalFilter.java
  src/main/java/org/rocksdb/AbstractWriteBatch.java
  src/main/java/org/rocksdb/AccessHint.java
  src/main/java/org/rocksdb/AdvancedColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/AdvancedMutableColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/BackgroundErrorReason.java
  src/main/java/org/rocksdb/BackupEngine.java
  src/main/java/org/rocksdb/BackupEngineOptions.java
  src/main/java/org/rocksdb/BackupInfo.java
  src/main/java/org/rocksdb/BlockBasedTableConfig.java
  src/main/java/org/rocksdb/BloomFilter.java
  src/main/java/org/rocksdb/BuiltinComparator.java
  src/main/java/org/rocksdb/ByteBufferGetStatus.java
  src/main/java/org/rocksdb/Cache.java
  src/main/java/org/rocksdb/CassandraCompactionFilter.java
  src/main/java/org/rocksdb/CassandraValueMergeOperator.java
  src/main/java/org/rocksdb/Checkpoint.java
  src/main/java/org/rocksdb/ChecksumType.java
  src/main/java/org/rocksdb/ClockCache.java
  src/main/java/org/rocksdb/ColumnFamilyDescriptor.java
  src/main/java/org/rocksdb/ColumnFamilyHandle.java
  src/main/java/org/rocksdb/ColumnFamilyMetaData.java
  src/main/java/org/rocksdb/ColumnFamilyOptions.java
  src/main/java/org/rocksdb/ColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/CompactRangeOptions.java
  src/main/java/org/rocksdb/CompactionJobInfo.java
  src/main/java/org/rocksdb/CompactionJobStats.java
  src/main/java/org/rocksdb/CompactionOptions.java
  src/main/java/org/rocksdb/CompactionOptionsFIFO.java
  src/main/java/org/rocksdb/CompactionOptionsUniversal.java
  src/main/java/org/rocksdb/CompactionPriority.java
  src/main/java/org/rocksdb/CompactionReason.java
  src/main/java/org/rocksdb/CompactionStopStyle.java
  src/main/java/org/rocksdb/CompactionStyle.java
  src/main/java/org/rocksdb/ComparatorOptions.java
  src/main/java/org/rocksdb/ComparatorType.java
  src/main/java/org/rocksdb/CompressionOptions.java
  src/main/java/org/rocksdb/CompressionType.java
  src/main/java/org/rocksdb/ConcurrentTaskLimiter.java
  src/main/java/org/rocksdb/ConcurrentTaskLimiterImpl.java
  src/main/java/org/rocksdb/ConfigOptions.java
  src/main/java/org/rocksdb/DBOptions.java
  src/main/java/org/rocksdb/DBOptionsInterface.java
  src/main/java/org/rocksdb/DataBlockIndexType.java
  src/main/java/org/rocksdb/DbPath.java
  src/main/java/org/rocksdb/DirectSlice.java
  src/main/java/org/rocksdb/EncodingType.java
  src/main/java/org/rocksdb/Env.java
  src/main/java/org/rocksdb/EnvOptions.java
  src/main/java/org/rocksdb/EventListener.java
  src/main/java/org/rocksdb/Experimental.java
  src/main/java/org/rocksdb/ExternalFileIngestionInfo.java
  src/main/java/org/rocksdb/FileOperationInfo.java
  src/main/java/org/rocksdb/Filter.java
  src/main/java/org/rocksdb/FlushJobInfo.java
  src/main/java/org/rocksdb/FlushOptions.java
  src/main/java/org/rocksdb/FlushReason.java
  src/main/java/org/rocksdb/HashLinkedListMemTableConfig.java
  src/main/java/org/rocksdb/HashSkipListMemTableConfig.java
  src/main/java/org/rocksdb/HistogramData.java
  src/main/java/org/rocksdb/HistogramType.java
  src/main/java/org/rocksdb/Holder.java
  src/main/java/org/rocksdb/IndexShorteningMode.java
  src/main/java/org/rocksdb/IndexType.java
  src/main/java/org/rocksdb/InfoLogLevel.java
  src/main/java/org/rocksdb/IngestExternalFileOptions.java
  src/main/java/org/rocksdb/KeyMayExist.java
  src/main/java/org/rocksdb/LRUCache.java
  src/main/java/org/rocksdb/LevelMetaData.java
  src/main/java/org/rocksdb/LiveFileMetaData.java
  src/main/java/org/rocksdb/LogFile.java
  src/main/java/org/rocksdb/Logger.java
  src/main/java/org/rocksdb/MemTableConfig.java
  src/main/java/org/rocksdb/MemTableInfo.java
  src/main/java/org/rocksdb/MemoryUsageType.java
  src/main/java/org/rocksdb/MemoryUtil.java
  src/main/java/org/rocksdb/MergeOperator.java
  src/main/java/org/rocksdb/MutableColumnFamilyOptions.java
  src/main/java/org/rocksdb/MutableColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/MutableDBOptions.java
  src/main/java/org/rocksdb/MutableDBOptionsInterface.java
  src/main/java/org/rocksdb/MutableOptionKey.java
  src/main/java/org/rocksdb/MutableOptionValue.java
  src/main/java/org/rocksdb/NativeComparatorWrapper.java
  src/main/java/org/rocksdb/NativeLibraryLoader.java
  src/main/java/org/rocksdb/OperationStage.java
  src/main/java/org/rocksdb/OperationType.java
  src/main/java/org/rocksdb/OptimisticTransactionDB.java
  src/main/java/org/rocksdb/OptimisticTransactionOptions.java
  src/main/java/org/rocksdb/OptionString.java
  src/main/java/org/rocksdb/Options.java
  src/main/java/org/rocksdb/OptionsUtil.java
  src/main/java/org/rocksdb/PersistentCache.java
  src/main/java/org/rocksdb/PlainTableConfig.java
  src/main/java/org/rocksdb/Priority.java
  src/main/java/org/rocksdb/Range.java
  src/main/java/org/rocksdb/RateLimiter.java
  src/main/java/org/rocksdb/RateLimiterMode.java
  src/main/java/org/rocksdb/ReadOptions.java
  src/main/java/org/rocksdb/ReadTier.java
  src/main/java/org/rocksdb/RemoveEmptyValueCompactionFilter.java
  src/main/java/org/rocksdb/RestoreOptions.java
  src/main/java/org/rocksdb/ReusedSynchronisationType.java
  src/main/java/org/rocksdb/RocksCallbackObject.java
  src/main/java/org/rocksdb/RocksDB.java
  src/main/java/org/rocksdb/RocksDBException.java
  src/main/java/org/rocksdb/RocksEnv.java
  src/main/java/org/rocksdb/RocksIterator.java
  src/main/java/org/rocksdb/RocksIteratorInterface.java
  src/main/java/org/rocksdb/RocksMemEnv.java
  src/main/java/org/rocksdb/RocksMutableObject.java
  src/main/java/org/rocksdb/RocksObject.java
  src/main/java/org/rocksdb/SanityLevel.java
  src/main/java/org/rocksdb/SizeApproximationFlag.java
  src/main/java/org/rocksdb/SkipListMemTableConfig.java
  src/main/java/org/rocksdb/Slice.java
  src/main/java/org/rocksdb/Snapshot.java
  src/main/java/org/rocksdb/SstFileManager.java
  src/main/java/org/rocksdb/SstFileMetaData.java
  src/main/java/org/rocksdb/SstFileReader.java
  src/main/java/org/rocksdb/SstFileReaderIterator.java
  src/main/java/org/rocksdb/SstFileWriter.java
  src/main/java/org/rocksdb/SstPartitionerFactory.java
  src/main/java/org/rocksdb/SstPartitionerFixedPrefixFactory.java
  src/main/java/org/rocksdb/StateType.java
  src/main/java/org/rocksdb/Statistics.java
  src/main/java/org/rocksdb/StatisticsCollector.java
  src/main/java/org/rocksdb/StatisticsCollectorCallback.java
  src/main/java/org/rocksdb/StatsCollectorInput.java
  src/main/java/org/rocksdb/StatsLevel.java
  src/main/java/org/rocksdb/Status.java
  src/main/java/org/rocksdb/StringAppendOperator.java
  src/main/java/org/rocksdb/TableFileCreationBriefInfo.java
  src/main/java/org/rocksdb/TableFileCreationInfo.java
  src/main/java/org/rocksdb/TableFileCreationReason.java
  src/main/java/org/rocksdb/TableFileDeletionInfo.java
  src/main/java/org/rocksdb/TableFilter.java
  src/main/java/org/rocksdb/TableFormatConfig.java
  src/main/java/org/rocksdb/TableProperties.java
  src/main/java/org/rocksdb/ThreadStatus.java
  src/main/java/org/rocksdb/ThreadType.java
  src/main/java/org/rocksdb/TickerType.java
  src/main/java/org/rocksdb/TimedEnv.java
  src/main/java/org/rocksdb/TraceOptions.java
  src/main/java/org/rocksdb/TraceWriter.java
  src/main/java/org/rocksdb/Transaction.java
  src/main/java/org/rocksdb/TransactionDB.java
  src/main/java/org/rocksdb/TransactionDBOptions.java
  src/main/java/org/rocksdb/TransactionLogIterator.java
  src/main/java/org/rocksdb/TransactionOptions.java
  src/main/java/org/rocksdb/TransactionalDB.java
  src/main/java/org/rocksdb/TransactionalOptions.java
  src/main/java/org/rocksdb/TtlDB.java
  src/main/java/org/rocksdb/TxnDBWritePolicy.java
  src/main/java/org/rocksdb/UInt64AddOperator.java
  src/main/java/org/rocksdb/VectorMemTableConfig.java
  src/main/java/org/rocksdb/WALRecoveryMode.java
  src/main/java/org/rocksdb/WBWIRocksIterator.java
  src/main/java/org/rocksdb/WalFileType.java
  src/main/java/org/rocksdb/WalFilter.java
  src/main/java/org/rocksdb/WalProcessingOption.java
  src/main/java/org/rocksdb/WriteBatch.java
  src/main/java/org/rocksdb/WriteBatchInterface.java
  src/main/java/org/rocksdb/WriteBatchWithIndex.java
  src/main/java/org/rocksdb/WriteBufferManager.java
  src/main/java/org/rocksdb/WriteOptions.java
  src/main/java/org/rocksdb/WriteStallCondition.java
  src/main/java/org/rocksdb/WriteStallInfo.java
  src/main/java/org/rocksdb/util/ByteUtil.java
  src/main/java/org/rocksdb/util/BytewiseComparator.java
  src/main/java/org/rocksdb/util/Environment.java
  src/main/java/org/rocksdb/util/IntComparator.java
  src/main/java/org/rocksdb/util/ReverseBytewiseComparator.java
  src/main/java/org/rocksdb/util/SizeUnit.java
)

set(JAVA_TEST_CLASSES
  src/test/java/org/rocksdb/AbstractTransactionTest.java
  src/test/java/org/rocksdb/BackupEngineOptionsTest.java
  src/test/java/org/rocksdb/BackupEngineTest.java
  src/test/java/org/rocksdb/BlobOptionsTest.java
  src/test/java/org/rocksdb/BlockBasedTableConfigTest.java
  src/test/java/org/rocksdb/BuiltinComparatorTest.java
  src/test/java/org/rocksdb/BytewiseComparatorRegressionTest.java
  src/test/java/org/rocksdb/CheckPointTest.java
  src/test/java/org/rocksdb/ClockCacheTest.java
  src/test/java/org/rocksdb/ColumnFamilyOptionsTest.java
  src/test/java/org/rocksdb/ColumnFamilyTest.java
  src/test/java/org/rocksdb/CompactRangeOptionsTest.java
  src/test/java/org/rocksdb/CompactionFilterFactoryTest.java
  src/test/java/org/rocksdb/CompactionJobInfoTest.java
  src/test/java/org/rocksdb/CompactionJobStatsTest.java
  src/test/java/org/rocksdb/CompactionOptionsFIFOTest.java
  src/test/java/org/rocksdb/CompactionOptionsTest.java
  src/test/java/org/rocksdb/CompactionOptionsUniversalTest.java
  src/test/java/org/rocksdb/CompactionPriorityTest.java
  src/test/java/org/rocksdb/CompactionStopStyleTest.java
  src/test/java/org/rocksdb/ComparatorOptionsTest.java
  src/test/java/org/rocksdb/CompressionOptionsTest.java
  src/test/java/org/rocksdb/CompressionTypesTest.java
  src/test/java/org/rocksdb/ConcurrentTaskLimiterTest.java
  src/test/java/org/rocksdb/DBOptionsTest.java
  src/test/java/org/rocksdb/DefaultEnvTest.java
  src/test/java/org/rocksdb/DirectSliceTest.java
  src/test/java/org/rocksdb/EnvOptionsTest.java
  src/test/java/org/rocksdb/EventListenerTest.java
  src/test/java/org/rocksdb/FilterTest.java
  src/test/java/org/rocksdb/FlushOptionsTest.java
  src/test/java/org/rocksdb/FlushTest.java
  src/test/java/org/rocksdb/InfoLogLevelTest.java
  src/test/java/org/rocksdb/IngestExternalFileOptionsTest.java
  src/test/java/org/rocksdb/KeyMayExistTest.java
  src/test/java/org/rocksdb/LRUCacheTest.java
  src/test/java/org/rocksdb/LoggerTest.java
  src/test/java/org/rocksdb/MemTableTest.java
  src/test/java/org/rocksdb/MemoryUtilTest.java
  src/test/java/org/rocksdb/MergeTest.java
  src/test/java/org/rocksdb/MixedOptionsTest.java
  src/test/java/org/rocksdb/MultiGetManyKeysTest.java
  src/test/java/org/rocksdb/MultiGetTest.java
  src/test/java/org/rocksdb/MutableColumnFamilyOptionsTest.java
  src/test/java/org/rocksdb/MutableDBOptionsTest.java
  src/test/java/org/rocksdb/MutableOptionsGetSetTest.java
  src/test/java/org/rocksdb/NativeComparatorWrapperTest.java
  src/test/java/org/rocksdb/NativeLibraryLoaderTest.java
  src/test/java/org/rocksdb/OptimisticTransactionDBTest.java
  src/test/java/org/rocksdb/OptimisticTransactionOptionsTest.java
  src/test/java/org/rocksdb/OptimisticTransactionTest.java
  src/test/java/org/rocksdb/OptionsTest.java
  src/test/java/org/rocksdb/OptionsUtilTest.java
  src/test/java/org/rocksdb/PlainTableConfigTest.java
  src/test/java/org/rocksdb/PlatformRandomHelper.java
  src/test/java/org/rocksdb/RateLimiterTest.java
  src/test/java/org/rocksdb/ReadOnlyTest.java
  src/test/java/org/rocksdb/ReadOptionsTest.java
  src/test/java/org/rocksdb/RocksDBExceptionTest.java
  src/test/java/org/rocksdb/RocksDBTest.java
  src/test/java/org/rocksdb/RocksIteratorTest.java
  src/test/java/org/rocksdb/RocksMemEnvTest.java
  src/test/java/org/rocksdb/RocksNativeLibraryResource.java
  src/test/java/org/rocksdb/SecondaryDBTest.java
  src/test/java/org/rocksdb/SliceTest.java
  src/test/java/org/rocksdb/SnapshotTest.java
  src/test/java/org/rocksdb/SstFileManagerTest.java
  src/test/java/org/rocksdb/SstFileReaderTest.java
  src/test/java/org/rocksdb/SstFileWriterTest.java
  src/test/java/org/rocksdb/SstPartitionerTest.java
  src/test/java/org/rocksdb/StatisticsCollectorTest.java
  src/test/java/org/rocksdb/StatisticsTest.java
  src/test/java/org/rocksdb/StatsCallbackMock.java
  src/test/java/org/rocksdb/TableFilterTest.java
  src/test/java/org/rocksdb/TimedEnvTest.java
  src/test/java/org/rocksdb/TransactionDBOptionsTest.java
  src/test/java/org/rocksdb/TransactionDBTest.java
  src/test/java/org/rocksdb/TransactionLogIteratorTest.java
  src/test/java/org/rocksdb/TransactionOptionsTest.java
  src/test/java/org/rocksdb/TransactionTest.java
  src/test/java/org/rocksdb/TtlDBTest.java
  src/test/java/org/rocksdb/Types.java
  src/test/java/org/rocksdb/VerifyChecksumsTest.java
  src/test/java/org/rocksdb/WALRecoveryModeTest.java
  src/test/java/org/rocksdb/WalFilterTest.java
  src/test/java/org/rocksdb/WriteBatchHandlerTest.java
  src/test/java/org/rocksdb/WriteBatchTest.java
  src/test/java/org/rocksdb/WriteBatchThreadedTest.java
  src/test/java/org/rocksdb/WriteBatchWithIndexTest.java
  src/test/java/org/rocksdb/WriteOptionsTest.java
  src/test/java/org/rocksdb/test/RemoveEmptyValueCompactionFilterFactory.java
  src/test/java/org/rocksdb/test/RocksJunitRunner.java
  src/test/java/org/rocksdb/test/RocksJUnitSuiteLister.java
  src/test/java/org/rocksdb/test/TestableEventListener.java
  src/test/java/org/rocksdb/util/ByteBufferAllocator.java
  src/test/java/org/rocksdb/util/BytewiseComparatorIntTest.java
  src/test/java/org/rocksdb/util/BytewiseComparatorTest.java
  src/test/java/org/rocksdb/util/CapturingWriteBatchHandler.java
  src/test/java/org/rocksdb/util/DirectByteBufferAllocator.java
  src/test/java/org/rocksdb/util/EnvironmentTest.java
  src/test/java/org/rocksdb/util/HeapByteBufferAllocator.java
  src/test/java/org/rocksdb/util/IntComparatorTest.java
  src/test/java/org/rocksdb/util/JNIComparatorTest.java
  src/test/java/org/rocksdb/util/ReverseBytewiseComparatorIntTest.java
  src/test/java/org/rocksdb/util/SizeUnitTest.java
  src/test/java/org/rocksdb/util/TestUtil.java
  src/test/java/org/rocksdb/util/WriteBatchGetter.java
)

include(FindJava)
include(UseJava)
find_package(JNI)

if(${Java_VERSION_MINOR} VERSION_LESS_EQUAL "7" AND ${Java_VERSION_MAJOR} STREQUAL "1")
  message(FATAL_ERROR "Detected Java 7 or older (${Java_VERSION_STRING}), minimum required version in now Java 8")
endif()

if(NOT EXISTS ${PROJECT_SOURCE_DIR}/java/classes)
  file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/java/classes)
endif()

set(JAVA_TEST_LIBDIR ${PROJECT_SOURCE_DIR}/java/test-libs)
if(NOT EXISTS ${JAVA_TEST_LIBDIR})
  file(MAKE_DIRECTORY ${JAVA_TEST_LIBDIR})
endif()

if (DEFINED CUSTOM_DEPS_URL)
  set(DEPS_URL ${CUSTOM_DEPS_URL}/)
else ()
  # Using a Facebook AWS account for S3 storage. (maven.org has a history
  # of failing in Travis builds.)
  set(DEPS_URL "https://rocksdb-deps.s3-us-west-2.amazonaws.com/jars")
endif()

set(JAVA_TMP_JAR ${JAVA_TEST_LIBDIR}/tmp.jar)
set(JAVA_DEP_LIBS junit-4.12.jar hamcrest-core-1.3.jar mockito-all-1.10.19.jar cglib-2.2.2.jar assertj-core-1.7.1.jar)
foreach(lib ${JAVA_DEP_LIBS})
  set(lib_path "${JAVA_TEST_LIBDIR}/${lib}")
  if(NOT EXISTS ${lib_path})
    message("Downloading ${lib_path}")
    file(DOWNLOAD "${DEPS_URL}/${lib}" ${JAVA_TMP_JAR} STATUS downloadStatus)
    list(GET downloadStatus 0 error_code)
    list(GET downloadStatus 1 error_message)
    if(NOT error_code EQUAL 0)
      message(FATAL_ERROR "Failed downloading ${lib_path}: ${error_message}")
    endif()
    file(RENAME ${JAVA_TMP_JAR} ${lib_path})
  endif()
  list(APPEND JAVA_TESTCLASSPATH ${lib_path})
endforeach()

set(JNI_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated-jni/include")
if(NOT EXISTS ${JNI_OUTPUT_DIR})
  file(MAKE_DIRECTORY ${JNI_OUTPUT_DIR})
endif()

# Java 1.8 or newer prepare the JAR...
message("Preparing Jar for JDK ${Java_VERSION_STRING}")
add_jar(
    ${PROJECT_NAME}jni_classes
    SOURCES
    ${JAVA_MAIN_CLASSES}
    INCLUDE_JARS ${JAVA_TESTCLASSPATH}
    GENERATE_NATIVE_HEADERS ${PROJECT_NAME}jni_headers DESTINATION ${JNI_OUTPUT_DIR}
)
add_jar(
    ${PROJECT_NAME}jni_tests
    SOURCES
    ${JAVA_TEST_CLASSES}
    INCLUDE_JARS ${PROJECT_NAME}jni_classes ${JAVA_TESTCLASSPATH}
    GENERATE_NATIVE_HEADERS ${PROJECT_NAME}jni_test_headers DESTINATION ${JNI_OUTPUT_DIR}
)

if(NOT MSVC)
  set_property(TARGET ${PROJECT_NAME} PROPERTY POSITION_INDEPENDENT_CODE ON)
  set_property(TARGET ${PROJECT_NAME}_plugins PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Windows")
  set(JNIPLATFORM "win")
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set(JNIPLATFORM "osx")
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  set(JNIPLATFORM "linux")
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  set(JNIPLATFORM "solaris")
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  set(JNIPLATFORM "freebsd")
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
  set(JNIPLATFORM "openbsd")
else()
  message(FATAL_ERROR "Unsupported system for JNI: ${CMAKE_SYSTEM_NAME}")
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "64")
  set(JNIPLATFORM "${JNIPLATFORM}64")
else()
  set(JNIPLATFORM "${JNIPLATFORM}32")
endif()

set(ROCKSDBJNI_LIB ${PROJECT_NAME}jni-${JNIPLATFORM})
add_library(${ROCKSDBJNI_LIB} SHARED ${JNI_NATIVE_SOURCES})
add_dependencies(${ROCKSDBJNI_LIB} ${PROJECT_NAME}jni_headers ${PROJECT_NAME}jni_test_headers)
target_include_directories(${ROCKSDBJNI_LIB} PRIVATE . "${CMAKE_CURRENT_BINARY_DIR}/generated-jni" ${JNI_INCLUDE_DIRS})
target_link_libraries(${ROCKSDBJNI_LIB} PRIVATE ${COMPRESSION_LIBS} ${PROJECT_NAME})

set_target_properties(
  ${ROCKSDBJNI_LIB}
  PROPERTIES
  COMPILE_PDB_OUTPUT_DIRECTORY ${CMAKE_CFG_INTDIR}
  COMPILE_PDB_NAME ${ROCKSDBJNI_LIB}.pdb
)
add_custom_target(${PROJECT_NAME}jni DEPENDS ${ROCKSDBJNI_LIB})

set(ROCKSDBJNI_JAR ${PROJECT_NAME}jni-${PROJECT_VERSION}.jar)
add_custom_command(OUTPUT "${ROCKSDBJNI_JAR}"
                   COMMAND "${CMAKE_COMMAND}" -E copy ${PROJECT_NAME}jni_classes.jar "${ROCKSDBJNI_JAR}"
                   COMMAND "${Java_JAR_EXECUTABLE}" -uf "${ROCKSDBJNI_JAR}" $<TARGET_FILE_NAME:${ROCKSDBJNI_LIB}>
                   DEPENDS ${ROCKSDBJNI_LIB}
                   WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
add_custom_target(${PROJECT_NAME}java DEPENDS ${ROCKSDBJNI_JAR})

if(WITH_TESTS)
  add_test(NAME jtest COMMAND ${Java_JAVA_EXECUTABLE}
           -cp "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}jni_tests.jar" :$<JOIN:${}
           org.rocksdb.test.RocksJunitRunner)
  add_dependencies(check rocksdbjni_tests)
endif()
