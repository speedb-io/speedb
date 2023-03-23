#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/checkpoint.h"

using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Snapshot;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::Transaction;
using ROCKSDB_NAMESPACE::TransactionDB;
using ROCKSDB_NAMESPACE::TransactionDBOptions;
using ROCKSDB_NAMESPACE::TransactionOptions;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::DB;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

int main() {
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  TransactionDBOptions dbOpt;
  TransactionDB* txn_db;

  options.create_if_missing = true;
  options.manual_wal_flush = true;
  //options.compression = rocksdb::CompressionType::kSnappyCompression;
  dbOpt.write_policy = rocksdb::TxnDBWritePolicy::WRITE_UNPREPARED;
  txn_options.deadlock_detect = false;
  txn_options.skip_concurrency_control = true;
  Status s;
  s = rocksdb::TransactionDB::Open(options, dbOpt, kDBPath, &txn_db);
  assert(s.ok());
  std::cout << "1 " << s.ToString() << std::endl;
  auto txn = txn_db->BeginTransaction(write_options, txn_options);
  txn->SetName("MyTransaction");

  s = txn->Prepare();
  assert(s.ok());
  std::cout << "2 " << s.ToString() << std::endl;
  s = txn->Commit();
  assert(s.ok());
  std::cout << "3 " << s.ToString() << std::endl;
  delete txn;

  // txn = txn_db->BeginTransaction(write_options, txn_options);
  // txn->Prepare();
  // assert(s.ok());
  // txn->Commit();
  // assert(s.ok());
  uint64_t sequence = txn_db->GetLatestSequenceNumber();


  s = txn_db->FlushWAL(false);
  
  
  if(s.ok()){
    std::cerr << "4 " << s.ToString() << std::endl;
  }
  rocksdb::Checkpoint* cp;
  s = rocksdb::Checkpoint::Create(txn_db, &cp);
  assert(s.ok());
  std::cout << "5 " << s.ToString() << std::endl;

  s = cp->CreateCheckpoint("/tmp/rocksdb_simple_example/checkpoint", 0, &sequence);
  if(s.ok()){
    std::cerr << "6 " << s.ToString() << std::endl;
  }

  delete txn_db;
  delete cp;
  
  options.create_if_missing = false;
  rocksdb::DB* roDB;
  s = rocksdb::TransactionDB::OpenForReadOnly(options, "/tmp/rocksdb_simple_example/checkpoint",
                                              &roDB, false);
  std::cout << "7 " << s.ToString() << std::endl;
  assert(s.ok());
  return 0;
}
