
#pragma once
#include "rocksdb/write_buffer_manager.h"
#include "db/db_impl/db_impl.h"
#include "db/column_family.h"

namespace ROCKSDB_NAMESPACE {
  
class SpdbMemoryManagerClient {
public:
 SpdbMemoryManagerClient(SpdbMemoryManager *_mem_manager, DBImpl *_db,
                         ColumnFamilyData *_cf)
   : mem_manager_(_mem_manager), db_(_db), cf_(_cf) {
    for (int i = 0; i< (int) ColumnFamilyData::WriteStallCause::kNumCauses; i++)
      delay_factor_[i] = 0;
    mem_manager_->RegisterClient(this);
  }

 ~SpdbMemoryManagerClient() {
   mem_manager_->UnRegisterClient(this);
  }
  
  size_t GetMutableDataSize() const {
    return cf()->mem()->ApproximateMemoryUsageFast();;
  }
  size_t GetImMutableDataSize() const {
    return cf()->imm()->ApproximateMemoryUsage();
  }
  size_t GetUnFlushDataSize() const {
    return cf()->imm()->ApproximateUnflushedMemTablesMemoryUsage();    
  }

  DBImpl *db() const {return db_;}
  ColumnFamilyData *cf() const {return cf_;}
  void SetDelay(size_t factor, ColumnFamilyData::WriteStallCause cause);

private:
  SpdbMemoryManager *mem_manager_;
  DBImpl *db_;
  ColumnFamilyData *cf_;  
  // future for delay support
  size_t delay_factor_[(int)ColumnFamilyData::WriteStallCause::kNumCauses];
  
  
};

}
