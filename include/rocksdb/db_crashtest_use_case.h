#pragma once

#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/use_case.h"
#include "rocksdb/utilities/customizable_util.h"

namespace ROCKSDB_NAMESPACE {

class DBCrashtestUseCase : public UseCase {
 public:
  DBCrashtestUseCase();
  static const char* kClassName() { return "rocksdb.DBCrashtestUseCase"; }
  const char* Name() const override { return kClassName(); }
  Status Populate(const ConfigOptions& cfg_opts, DBOptions& db_opts) override {
    return Status::OK();
  }
  Status Populate(const ConfigOptions& cfg_opts,
                  ColumnFamilyOptions& cf_opts) override {
    return Status::OK();
  }
  Status Populate(const ConfigOptions& cfg_opts,
                  BlockBasedTableOptions& bbt_opts) const {
    return Status::OK();
  }
};

class SimpleDefaultParams : public DBCrashtestUseCase {
 public:
  SimpleDefaultParams();
  static const char* kClassName() { return "rocksdb.SimpleDefaultParams"; }
  const char* Name() const override { return kClassName(); }
};

class TxnParams : public DBCrashtestUseCase {
 public:
  TxnParams();
  static const char* kClassName() { return "rocksdb.TxnParams"; }
  const char* Name() const override { return kClassName(); }
};

class BestEffortsRecoveryParams : public DBCrashtestUseCase {
 public:
  BestEffortsRecoveryParams();
  static const char* kClassName() { return "rocksdb.BestEffortsRecoveryParams"; }
  const char* Name() const override { return kClassName(); }
};

class BlobParams : public DBCrashtestUseCase {
 public:
  BlobParams();
  static const char* kClassName() { return "rocksdb.BlobParams"; }
  const char* Name() const override { return kClassName(); }
};

class TieredParams : public DBCrashtestUseCase {
 public:
  TieredParams();
  static const char* kClassName() { return "rocksdb.TieredParams"; }
  const char* Name() const override { return kClassName(); }
};

class MultiopsTxnDefaultParams : public DBCrashtestUseCase {
 public:
  MultiopsTxnDefaultParams();
  static const char* kClassName() { return "rocksdb.MultiopsTxnDefaultParams"; }
  const char* Name() const override { return kClassName(); }
};
}  // namespace ROCKSDB_NAMESPACE
