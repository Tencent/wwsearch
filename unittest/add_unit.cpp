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

class IndexAddTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;

 public:
  IndexAddTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path = std::string("/tmp/unit_") + std::string("add");
    index->Config().SetLogLevel(g_debug ? wwsearch::kSearchLogLevelDebug
                                        : wwsearch::kSearchLogLevelError);
    auto status = index->Open(g_use_rocksdb, g_use_compression);
    ASSERT_TRUE(status.GetCode() == 0);
  }

  static void TearDownTestCase() {
    if (index != nullptr) {
      auto status = index->vdb_->DropDB();
      EXPECT_EQ(0, status.GetCode());
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

DefaultIndexWrapper *IndexAddTest::index = nullptr;
DocumentID IndexAddTest::document_id = 1;

TEST_F(IndexAddTest, AddOneDocument) {
  ASSERT_EQ(documents.size(), 0);
  for (int i = 0; i < 10; i++)
    documents.push_back(TestUtil::NewDocument(GetDocumentID(), "a", 1, 1, 2));

  bool ret =
      index->index_writer_->AddDocuments(table, documents, nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }
}

TEST_F(IndexAddTest, AddTwiceDocument) {
  ASSERT_EQ(documents.size(), 0);

  for (int i = 0; i < 10; i++)
    documents.push_back(TestUtil::NewDocument(GetDocumentID(), "a", 1, 1, 2));

  bool ret =
      index->index_writer_->AddDocuments(table, documents, nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }
  ret = index->index_writer_->AddDocuments(table, documents, nullptr, nullptr);
  EXPECT_FALSE(ret);
  for (auto du : documents) {
    EXPECT_EQ(wwsearch::kDocumentExistStatus, du->Status().GetCode());
  }
}

}  // namespace wwsearch
