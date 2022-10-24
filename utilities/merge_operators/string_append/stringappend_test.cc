//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

/**
 * An persistent map : key -> (list of strings), using rocksdb merge.
 * This file is a test-harness / use-case for the StringAppendOperator.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook, Inc.
*/

#include "utilities/merge_operators/string_append/stringappend.h"

#include <iostream>
#include <map>
#include <tuple>

#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend2.h"


namespace ROCKSDB_NAMESPACE {

/// StringLists represents a set of string-lists, each with a key-index.
/// Supports Append(list, string) and Get(list)
class StringLists {
 public:
  StringLists() : dbname_(test::PerThreadDBPath("stringappend_test")), merge_option_(), get_option_() {
  }

  ~StringLists() {
    Close(true);
  }

  // Append string val onto the list defined by key; return true on success
  bool Append(const std::string& key, const std::string& val){
    Slice valSlice(val.data(), val.size());
    auto s = db_->Merge(merge_option_, key, valSlice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << "ERROR " << s.ToString() << std::endl;
      return false;
    }
  }

  // Returns the list of strings associated with key (or "" if does not exist)
  bool Get(const std::string& key, std::string* const result){
    assert(result != nullptr); // we should have a place to store the result
    auto s = db_->Get(get_option_, key, result);

    if (s.ok()) {
      return true;
    }

    // Either key does not exist, or there is some error.
    *result = "";       // Always return empty string (just for convention)

    //NotFound is okay; just return empty (similar to std::map)
    //But network or db errors, etc, should fail the test (or at least yell)
    if (!s.IsNotFound()) {
      std::cerr << "ERROR " << s.ToString() << std::endl;
    }

    // Always return false if s.ok() was not true
    return false;
  }

  void OpenDb(bool use_ttl, const std::string& delim) {
    Options options;
    options.create_if_missing = true;
    MergeOperator* mergeOperator;
    if (delim.size() == 1) {
      mergeOperator = new StringAppendOperator(delim[0]);
    } else {
      mergeOperator = new StringAppendOperator(delim);
    }
    options.merge_operator.reset(mergeOperator);

    Close(false);
#ifndef ROCKSDB_LITE  // TtlDb is not supported in Lite
    // Open a TtlDB with a non-associative StringAppendTESTOperator
    if (use_ttl) {
      DBWithTTL* db;
      EXPECT_OK(DBWithTTL::Open(options, dbname_, &db, 123456));
      db_ = db;
    } else
#endif  // !ROCKSDB_LITE
    {
      // OpenDb opens a (possibly new) rocksdb database with a StringAppendOperator
      DB* db;
      EXPECT_OK(DB::Open(options, dbname_, &db));
      (void)use_ttl;
      db_ = db;
    }
    options_ = options;
  }

  DB* db() { return db_; }

 private:
  void Close(bool destroy) {
    if (db_ != nullptr) {
      delete db_;
      if (destroy) {
        ASSERT_OK(DestroyDB(dbname_, options_));
      }
    }
  }

  DB* db_ = nullptr;
  Options options_;
  // Path to the database on file system
  const std::string dbname_;

  WriteOptions merge_option_;
  ReadOptions get_option_;
};


// The class for unit-testing
class StringAppendOperatorTest : public testing::Test,
                                 public ::testing::WithParamInterface<bool> {
 public:
  void SetUp() override {
#ifndef ROCKSDB_LITE  // TtlDb is not supported in Lite
    use_ttl_ = GetParam();
    if (use_ttl_) {
      fprintf(stderr, "Running tests with ttl db and generic operator.\n");
      return;
    }
#endif  // !ROCKSDB_LITE
    fprintf(stderr, "Running tests with regular db and operator.\n");
    use_ttl_ = false;
  }

 protected:
  void OpenDb(const std::string& delim) {
    slists_.OpenDb(use_ttl_, delim);
  }

  DB* db() {
    return slists_.db();
  }

  StringLists slists_;

 private:
  bool use_ttl_;
};

// THE TEST CASES BEGIN HERE

TEST_P(StringAppendOperatorTest, IteratorTest) {
  OpenDb(",");

  slists_.Append("k1", "v1");
  slists_.Append("k1", "v2");
  slists_.Append("k1", "v3");

  slists_.Append("k2", "a1");
  slists_.Append("k2", "a2");
  slists_.Append("k2", "a3");

  std::string res;
  std::unique_ptr<ROCKSDB_NAMESPACE::Iterator> it(
      db()->NewIterator(ReadOptions()));
  std::string k1("k1");
  std::string k2("k2");
  bool first = true;
  for (it->Seek(k1); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "v1,v2,v3");
      first = false;
    } else {
      ASSERT_EQ(res, "a1,a2,a3");
    }
  }
  slists_.Append("k2", "a4");
  slists_.Append("k1", "v4");

  // Snapshot should still be the same. Should ignore a4 and v4.
  first = true;
  for (it->Seek(k1); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "v1,v2,v3");
      first = false;
    } else {
      ASSERT_EQ(res, "a1,a2,a3");
    }
  }


  // Should release the snapshot and be aware of the new stuff now
  it.reset(db()->NewIterator(ReadOptions()));
  first = true;
  for (it->Seek(k1); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "v1,v2,v3,v4");
      first = false;
    } else {
      ASSERT_EQ(res, "a1,a2,a3,a4");
    }
  }

  // start from k2 this time.
  for (it->Seek(k2); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "v1,v2,v3,v4");
      first = false;
    } else {
      ASSERT_EQ(res, "a1,a2,a3,a4");
    }
  }

  slists_.Append("k3", "g1");

  it.reset(db()->NewIterator(ReadOptions()));
  first = true;
  std::string k3("k3");
  for(it->Seek(k2); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "a1,a2,a3,a4");
      first = false;
    } else {
      ASSERT_EQ(res, "g1");
    }
  }
  for(it->Seek(k3); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      // should not be hit
      ASSERT_EQ(res, "a1,a2,a3,a4");
      first = false;
    } else {
      ASSERT_EQ(res, "g1");
    }
  }
}

TEST_P(StringAppendOperatorTest, SimpleTest) {
  OpenDb(",");

  slists_.Append("k1", "v1");
  slists_.Append("k1", "v2");
  slists_.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists_.Get("k1", &res));
  ASSERT_EQ(res, "v1,v2,v3");
}

TEST_P(StringAppendOperatorTest, SimpleDelimiterTest) {
  OpenDb("|");

  slists_.Append("k1", "v1");
  slists_.Append("k1", "v2");
  slists_.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists_.Get("k1", &res));
  ASSERT_EQ(res, "v1|v2|v3");
}

TEST_P(StringAppendOperatorTest, EmptyDelimiterTest) {
  OpenDb("");

  slists_.Append("k1", "v1");
  slists_.Append("k1", "v2");
  slists_.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists_.Get("k1", &res));
  ASSERT_EQ(res, "v1v2v3");
}

TEST_P(StringAppendOperatorTest, MultiCharDelimiterTest) {
  OpenDb("<>");

  slists_.Append("k1", "v1");
  slists_.Append("k1", "v2");
  slists_.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists_.Get("k1", &res));
  ASSERT_EQ(res, "v1<>v2<>v3");
}

TEST_P(StringAppendOperatorTest, DelimiterIsDefensivelyCopiedTest) {
  std::string delimiter = "<>";
  OpenDb(delimiter);

  slists_.Append("k1", "v1");
  slists_.Append("k1", "v2");
  delimiter.clear();
  slists_.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists_.Get("k1", &res));
  ASSERT_EQ(res, "v1<>v2<>v3");
}

TEST_P(StringAppendOperatorTest, OneValueNoDelimiterTest) {
  OpenDb("!");

  slists_.Append("random_key", "single_val");

  std::string res;
  ASSERT_TRUE(slists_.Get("random_key", &res));
  ASSERT_EQ(res, "single_val");
}

TEST_P(StringAppendOperatorTest, VariousKeys) {
  OpenDb("\n");

  slists_.Append("c", "asdasd");
  slists_.Append("a", "x");
  slists_.Append("b", "y");
  slists_.Append("a", "t");
  slists_.Append("a", "r");
  slists_.Append("b", "2");
  slists_.Append("c", "asdasd");

  std::string a, b, c;
  bool sa, sb, sc;
  sa = slists_.Get("a", &a);
  sb = slists_.Get("b", &b);
  sc = slists_.Get("c", &c);

  ASSERT_TRUE(sa && sb && sc); // All three keys should have been found

  ASSERT_EQ(a, "x\nt\nr");
  ASSERT_EQ(b, "y\n2");
  ASSERT_EQ(c, "asdasd\nasdasd");
}

// Generate semi random keys/words from a small distribution.
TEST_P(StringAppendOperatorTest, RandomMixGetAppend) {
  OpenDb(" ");

  // Generate a list of random keys and values
  const int kWordCount = 15;
  std::string words[] = {"sdasd", "triejf", "fnjsdfn", "dfjisdfsf", "342839",
                         "dsuha", "mabuais", "sadajsid", "jf9834hf", "2d9j89",
                         "dj9823jd", "a", "dk02ed2dh", "$(jd4h984$(*", "mabz"};
  const int kKeyCount = 6;
  std::string keys[] = {"dhaiusdhu", "denidw", "daisda", "keykey", "muki",
                        "shzassdianmd"};

  // Will store a local copy of all data in order to verify correctness
  std::map<std::string, std::string> parallel_copy;

  // Generate a bunch of random queries (Append and Get)!
  enum query_t  { APPEND_OP, GET_OP, NUM_OPS };
  Random randomGen(1337);       //deterministic seed; always get same results!

  const int kNumQueries = 30;
  for (int q=0; q<kNumQueries; ++q) {
    // Generate a random query (Append or Get) and random parameters
    query_t query = (query_t)randomGen.Uniform((int)NUM_OPS);
    std::string key = keys[randomGen.Uniform((int)kKeyCount)];
    std::string word = words[randomGen.Uniform((int)kWordCount)];

    // Apply the query and any checks.
    if (query == APPEND_OP) {

      // Apply the rocksdb test-harness Append defined above
      slists_.Append(key, word);  //apply the rocksdb append

      // Apply the similar "Append" to the parallel copy
      if (parallel_copy[key].size() > 0) {
        parallel_copy[key] += " " + word;
      } else {
        parallel_copy[key] = word;
      }

    } else if (query == GET_OP) {
      // Assumes that a non-existent key just returns <empty>
      std::string res;
      slists_.Get(key, &res);
      ASSERT_EQ(res, parallel_copy[key]);
    }

  }
}

TEST_P(StringAppendOperatorTest, BIGRandomMixGetAppend) {
  OpenDb(" ");

  // Generate a list of random keys and values
  const int kWordCount = 15;
  std::string words[] = {"sdasd", "triejf", "fnjsdfn", "dfjisdfsf", "342839",
                         "dsuha", "mabuais", "sadajsid", "jf9834hf", "2d9j89",
                         "dj9823jd", "a", "dk02ed2dh", "$(jd4h984$(*", "mabz"};
  const int kKeyCount = 6;
  std::string keys[] = {"dhaiusdhu", "denidw", "daisda", "keykey", "muki",
                        "shzassdianmd"};

  // Will store a local copy of all data in order to verify correctness
  std::map<std::string, std::string> parallel_copy;

  // Generate a bunch of random queries (Append and Get)!
  enum query_t  { APPEND_OP, GET_OP, NUM_OPS };
  Random randomGen(9138204);       // deterministic seed

  const int kNumQueries = 1000;
  for (int q=0; q<kNumQueries; ++q) {
    // Generate a random query (Append or Get) and random parameters
    query_t query = (query_t)randomGen.Uniform((int)NUM_OPS);
    std::string key = keys[randomGen.Uniform((int)kKeyCount)];
    std::string word = words[randomGen.Uniform((int)kWordCount)];

    //Apply the query and any checks.
    if (query == APPEND_OP) {

      // Apply the rocksdb test-harness Append defined above
      slists_.Append(key, word);  //apply the rocksdb append

      // Apply the similar "Append" to the parallel copy
      if (parallel_copy[key].size() > 0) {
        parallel_copy[key] += " " + word;
      } else {
        parallel_copy[key] = word;
      }

    } else if (query == GET_OP) {
      // Assumes that a non-existent key just returns <empty>
      std::string res;
      slists_.Get(key, &res);
      ASSERT_EQ(res, parallel_copy[key]);
    }

  }
}

TEST_P(StringAppendOperatorTest, PersistentVariousKeys) {
  // Perform the following operations in limited scope
  {
    OpenDb("\n");

    slists_.Append("c", "asdasd");
    slists_.Append("a", "x");
    slists_.Append("b", "y");
    slists_.Append("a", "t");
    slists_.Append("a", "r");
    slists_.Append("b", "2");
    slists_.Append("c", "asdasd");

    std::string a, b, c;
    ASSERT_TRUE(slists_.Get("a", &a));
    ASSERT_TRUE(slists_.Get("b", &b));
    ASSERT_TRUE(slists_.Get("c", &c));

    ASSERT_EQ(a, "x\nt\nr");
    ASSERT_EQ(b, "y\n2");
    ASSERT_EQ(c, "asdasd\nasdasd");
  }

  // Reopen the database (the previous changes should persist / be remembered)
  {
    OpenDb("\n");

    slists_.Append("c", "bbnagnagsx");
    slists_.Append("a", "sa");
    slists_.Append("b", "df");
    slists_.Append("a", "gh");
    slists_.Append("a", "jk");
    slists_.Append("b", "l;");
    slists_.Append("c", "rogosh");

    // The previous changes should be on disk (L0)
    // The most recent changes should be in memory (MemTable)
    // Hence, this will test both Get() paths.
    std::string a, b, c;
    ASSERT_TRUE(slists_.Get("a", &a));
    ASSERT_TRUE(slists_.Get("b", &b));
    ASSERT_TRUE(slists_.Get("c", &c));

    ASSERT_EQ(a, "x\nt\nr\nsa\ngh\njk");
    ASSERT_EQ(b, "y\n2\ndf\nl;");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx\nrogosh");
  }

  // Reopen the database (the previous changes should persist / be remembered)
  {
    OpenDb("\n");

    // All changes should be on disk. This will test VersionSet Get()
    std::string a, b, c;
    ASSERT_TRUE(slists_.Get("a", &a));
    ASSERT_TRUE(slists_.Get("b", &b));
    ASSERT_TRUE(slists_.Get("c", &c));

    ASSERT_EQ(a, "x\nt\nr\nsa\ngh\njk");
    ASSERT_EQ(b, "y\n2\ndf\nl;");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx\nrogosh");
  }
}

TEST_P(StringAppendOperatorTest, PersistentFlushAndCompaction) {
  // Perform the following operations in limited scope
  {
    OpenDb("\n");
    std::string a, b, c;

    // Append, Flush, Get
    slists_.Append("c", "asdasd");
    ASSERT_OK(db()->Flush(ROCKSDB_NAMESPACE::FlushOptions()));
    ASSERT_TRUE(slists_.Get("c", &c));
    ASSERT_EQ(c, "asdasd");

    // Append, Flush, Append, Get
    slists_.Append("a", "x");
    slists_.Append("b", "y");
    ASSERT_OK(db()->Flush(ROCKSDB_NAMESPACE::FlushOptions()));
    slists_.Append("a", "t");
    slists_.Append("a", "r");
    slists_.Append("b", "2");

    ASSERT_TRUE(slists_.Get("a", &a));
    ASSERT_EQ(a, "x\nt\nr");

    ASSERT_TRUE(slists_.Get("b", &b));
    ASSERT_EQ(b, "y\n2");

    // Append, Get
    ASSERT_TRUE(slists_.Append("c", "asdasd"));
    ASSERT_TRUE(slists_.Append("b", "monkey"));

    ASSERT_TRUE(slists_.Get("a", &a));
    ASSERT_TRUE(slists_.Get("b", &b));
    ASSERT_TRUE(slists_.Get("c", &c));

    ASSERT_EQ(a, "x\nt\nr");
    ASSERT_EQ(b, "y\n2\nmonkey");
    ASSERT_EQ(c, "asdasd\nasdasd");
  }

  // Reopen the database (the previous changes should persist / be remembered)
  {
    OpenDb("\n");
    std::string a, b, c;

    // Get (Quick check for persistence of previous database)
    ASSERT_TRUE(slists_.Get("a", &a));
    ASSERT_EQ(a, "x\nt\nr");

    //Append, Compact, Get
    slists_.Append("c", "bbnagnagsx");
    slists_.Append("a", "sa");
    slists_.Append("b", "df");
    ASSERT_OK(db()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_TRUE(slists_.Get("a", &a));
    ASSERT_TRUE(slists_.Get("b", &b));
    ASSERT_TRUE(slists_.Get("c", &c));
    ASSERT_EQ(a, "x\nt\nr\nsa");
    ASSERT_EQ(b, "y\n2\nmonkey\ndf");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx");

    // Append, Get
    slists_.Append("a", "gh");
    slists_.Append("a", "jk");
    slists_.Append("b", "l;");
    slists_.Append("c", "rogosh");
    ASSERT_TRUE(slists_.Get("a", &a));
    ASSERT_TRUE(slists_.Get("b", &b));
    ASSERT_TRUE(slists_.Get("c", &c));
    ASSERT_EQ(a, "x\nt\nr\nsa\ngh\njk");
    ASSERT_EQ(b, "y\n2\nmonkey\ndf\nl;");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx\nrogosh");

    // Compact, Get
    ASSERT_OK(db()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_EQ(a, "x\nt\nr\nsa\ngh\njk");
    ASSERT_EQ(b, "y\n2\nmonkey\ndf\nl;");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx\nrogosh");

    // Append, Flush, Compact, Get
    slists_.Append("b", "afcg");
    ASSERT_OK(db()->Flush(ROCKSDB_NAMESPACE::FlushOptions()));
    ASSERT_OK(db()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_TRUE(slists_.Get("b", &b));
    ASSERT_EQ(b, "y\n2\nmonkey\ndf\nl;\nafcg");
  }
}

TEST_P(StringAppendOperatorTest, SimpleTestNullDelimiter) {
  OpenDb(std::string(1, '\0'));

  slists_.Append("k1", "v1");
  slists_.Append("k1", "v2");
  slists_.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists_.Get("k1", &res));

  // Construct the desired string. Default constructor doesn't like '\0' chars.
  std::string checker("v1,v2,v3");    // Verify that the string is right size.
  checker[2] = '\0';                  // Use null delimiter instead of comma.
  checker[5] = '\0';
  ASSERT_EQ(checker.size(), 8);  // Verify it is still the correct size

  // Check that the rocksdb result string matches the desired string
  ASSERT_EQ(res.size(), checker.size());
  ASSERT_EQ(res, checker);
}

INSTANTIATE_TEST_CASE_P(StringAppendOperatorTest, StringAppendOperatorTest,
                        testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
