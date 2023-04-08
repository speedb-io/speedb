#pragma once

#include "rocksdb/status.h"
#include "rocksdb/use_case.h"

class DBCrashtestOptions : public UseCase {
 public:
  static DBCrashtestOptions& GetInstance();
  virtual const char* Name() const override;
  virtual Status Populate(Options& options);
  virtual Status Validate(const Options& options);
  virtual Status Validate(const DBOptions& db_opts, 
                          const ColumnFamilyOptions& cf_opts);

 private:
  DBCrashtestOptions();
  ~DBCrashtestOptions();
  DBCrashtestOptions(const DBCrashtestOptions&);
  const DBCrashtestOptions& operator=(const DBCrashtestOptions&)
};