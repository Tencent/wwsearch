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

class IndexDeleteTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;

 public:
  IndexDeleteTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path = std::string("/tmp/unit_") + std::string("delete");
    index->Config().SetLogLevel(g_debug ? wwsearch::kSearchLogLevelDebug
                                        : wwsearch::kSearchLogLevelError);
    auto status = index->Open(g_use_rocksdb, g_use_compression);
    ASSERT_TRUE(status.GetCode() == 0);
  }

  static void TearDownTestCase() {
    if (index != nullptr) {
      index->vdb_->DropDB();
      delete index;
      index = nullptr;
    }
  }

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

DefaultIndexWrapper *IndexDeleteTest::index = nullptr;
DocumentID IndexDeleteTest::document_id = 1;
TEST_F(IndexDeleteTest, AddOneDocument) {
  for (int i = 0; i < 10; i++)
    documents.push_back(TestUtil::NewDocument(GetDocumentID(), "a", 1, 1, 2));

  bool ret =
      index->index_writer_->DeleteDocuments(table, documents, nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(wwsearch::kDocumentNotExistStatus, du->Status().GetCode());
  }
}

TEST_F(IndexDeleteTest, AddTwiceDocument) {
  for (int i = 0; i < 10; i++)
    documents.push_back(TestUtil::NewDocument(GetDocumentID(), "a", 1, 1, 2));

  bool ret =
      index->index_writer_->AddDocuments(table, documents, nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }
  ret =
      index->index_writer_->DeleteDocuments(table, documents, nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }
}

}  // namespace wwsearch
