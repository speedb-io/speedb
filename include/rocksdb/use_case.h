#pragma once

#include "rocksdb/status.h"

class UseCase {
 public:
  // The unique name for this use case.
  virtual const char* Name() const override = 0;
  // Populate the Options according to the rules for the given purpose.
  virtual Status Populate(Options& options);
  // Return whether or not this specific use case cares about validation.
  // In the case of multiple use cases, where one of them already
  // passed validation, then the options are likely valid
  // and having another validation is redundant.
  // True by default.
  virtual bool ShouldValidate() const {
    return true;
  }
  // Validates the Options according to the rules for the given purpose.
  // If the options are not valid for this use case, an error status
  // is returned.
  virtual Status Validate(const Options& options);
  virtual Status Validate(const DBOptions& db_opts, 
                          const ColumnFamilyOptions & cf_opts);
};