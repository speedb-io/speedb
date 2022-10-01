#pragma once

#include <memory>

#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

namespace ddm_tests {

inline constexpr size_t kWBMQuota = 1024 * 1024 * 256;  // 256MB

std::shared_ptr<WriteBufferManager> CreateWBM() {
  // Assuming initiate_flushes is true by default 
  std::shared_ptr<WriteBufferManager> wbm(new WriteBufferManager( kWBMQuota, 
                                                                  {} /* cache */,
                                                                  true /* allow_delays_and_stalls */));
  return wbm;
}

void OptimizeOptions(std::shared_ptr<WriteBufferManager> wbm, Options& options) {
  options.db_write_buffer_size = wbm->buffer_size();
  options.write_buffer_size = options.db_write_buffer_size / 2;
  options.max_write_buffer_number = (options.db_write_buffer_size / options.write_buffer_size + 1) * 2;
  options.min_write_buffer_number_to_merge = options.max_write_buffer_number / 2;
  options.write_buffer_manager = wbm;

  options.delayed_write_rate = 256 * 1024 * 1024; // 256MBPS
}

ColumnFamilyOptions GetCfOptionsFromOptions(const Options& options) {
  ColumnFamilyOptions cf_options;
  cf_options.write_buffer_size = options.write_buffer_size;
  cf_options.max_write_buffer_number = options.max_write_buffer_number;
  cf_options.min_write_buffer_number_to_merge = options.min_write_buffer_number_to_merge;
  return cf_options;
}

} // namespace ddm_tests
}