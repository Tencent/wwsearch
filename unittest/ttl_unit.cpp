/*
 * Tencent is pleased to support the open source community by making wwsearch
 * available.
 *
 * Copyright (C) 2018-present Tencent. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

#include <gtest/gtest.h>
#include "include/index_wrapper.h"
#include "unittest_util.h"

namespace wwsearch {

class DBTTLTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;

 public:
  DBTTLTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void OpenDB() {
    index = new DefaultIndexWrapper();
    index->DBParams().path = std::string("/tmp/unit_") + std::string("ttl");
    index->Config().SetLogLevel(g_debug ? wwsearch::kSearchLogLevelDebug
                                        : wwsearch::kSearchLogLevelError);
    auto status = index->Open(g_use_rocksdb, g_use_compression);
    ASSERT_TRUE(status.GetCode() == 0);
  }

  static void SetUpTestCase() { OpenDB(); }

  static void CloseDB(bool drop = false) {
    if (index != nullptr) {
      if (drop) {
        auto status = index->vdb_->DropDB();
        EXPECT_EQ(0, status.GetCode());
      }
      delete index;
      index = nullptr;
    }
  }
  static void TearDownTestCase() { CloseDB(true); }

  virtual void SetUp() override { table.partition_set++; }

  virtual void TearDown() override {
    for (auto du : documents) {
      delete du;
    }
    documents.clear();
  }

  uint64_t GetDocumentID() { return document_id++; }

 private:
};

DefaultIndexWrapper *DBTTLTest::index = nullptr;
DocumentID DBTTLTest::document_id = 1;

TEST_F(DBTTLTest, TTLDeleteRecords) {
  auto db = index->vdb_;

  std::string key, value;
  auto write_buffer = db->NewWriteBuffer(nullptr);
  ASSERT_TRUE(write_buffer->Put(kPaxosLogColumn, "key", "value").OK());
  ASSERT_TRUE(db->FlushBuffer(write_buffer).OK());
  db->ReleaseWriteBuffer(write_buffer);
  key = "key";
  ASSERT_TRUE(db->Get(kPaxosLogColumn, key, value, nullptr).OK());
  // sleep(5);
  // WARNING: TTL only trigger when compaction.
  // see: https://github.com/facebook/rocksdb/wiki/Time-to-Live
  // so it does not work in this test case.
  // CloseDB();
  // OpenDB();
  // auto status = db->Get(kPaxosLogColumn, key, value);
  // ASSERT_FALSE(status.OK());
  // ASSERT_TRUE(status.DocumentNotExist());
}

}  // namespace wwsearch
