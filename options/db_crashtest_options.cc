#include <random>
#include "options/db_crashtest_options.h"

// ignore this ugly way of generating random numbers in c++
std::random_device dev;
std::mt19937 rng(dev());
std::uniform_int_distribution<std::mt19937::result_type> bytes_per_sync_dist(0, 262144);
std::uniform_int_distribution<std::mt19937::result_type> wal_bytes_per_sync_dist(0, 524288);

DBCrashtestOptions& DBCrashtestOptions::GetInstance() {
  // Not thread safe, could be constructed twice or used by
  // one thread before it has been fully initialized by the
  // other thread.
  // Hence the call to this method should be made
  // somewhere where there is thread safety.
  static DBCrashtestOptions instance;
  return instance;
}

const char* DBCrashtestOptions::Name() {
  return "DBCrashtestOptions";
}

Status DBCrashtestOptions::Populate(Options& options) {
  // some options
  options.bytes_per_sync = bytes_per_sync_dist(rng)
  options.wal_bytes_per_sync = wal_bytes_per_sync_dist(rng)
}

Status DBCrashtestOptions::Validate(const Options& options) {
  // TODO
}

Status DBCrashtestOptions::Validate(const DBOptions& db_opts, 
                                    const ColumnFamilyOptions& cf_opts) {
  // TODO
}