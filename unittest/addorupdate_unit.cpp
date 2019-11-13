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

class IndexAddOrUpdateTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;

 public:
  IndexAddOrUpdateTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path =
        std::string("/tmp/unit_") + std::string("addorupdate");
    index->Config().SetLogLevel(g_debug ? wwsearch::kSearchLogLevelDebug
                                        : wwsearch::kSearchLogLevelError);
    auto status = index->Open(g_use_rocksdb, g_use_compression);
    assert(status.GetCode() == 0);
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

DefaultIndexWrapper *IndexAddOrUpdateTest::index = nullptr;
DocumentID IndexAddOrUpdateTest::document_id = 1;

TEST_F(IndexAddOrUpdateTest, AddOneDocument) {
  for (int i = 0; i < 10; i++)
    documents.push_back(
        TestUtil::NewDocument(GetDocumentID(), "a", 999, 999, 999));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }
}

TEST_F(IndexAddOrUpdateTest, AddTwiceDocument) {
  for (int i = 0; i < 10; i++)
    documents.push_back(
        TestUtil::NewDocument(GetDocumentID(), "a", 999, 999, 999));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
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

//
TEST_F(IndexAddOrUpdateTest, AddOrRemoveFieldThenQuery) {
  auto document_update =
      TestUtil::NewDocument(GetDocumentID(), "old value", 1, 10, 100);
  auto doc_id = document_update->New().ID();
  documents.push_back(document_update);
  {
    bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                          nullptr, nullptr);
    EXPECT_TRUE(ret);
    for (auto du : documents) {
      EXPECT_EQ(0, du->Status().GetCode());
      delete du;
    }
    documents.clear();
  }

  document_update =
      TestUtil::NewDocument(GetDocumentID(), "old value", 1, 10, 100);
  document_update->New().SetID(doc_id);
  documents.push_back(document_update);
  {
    // update
    document_update->New().FindField(1)->SetString("new value");
    document_update->New().FindField(2)->SetUint32(2);

    InitStringField(document_update->New().AddField(), 5, "add field");
    InitUint32Field(document_update->New().AddField(), 6, 20);
    bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                          nullptr, nullptr);
    EXPECT_TRUE(ret);
    for (auto du : documents) {
      EXPECT_EQ(0, du->Status().GetCode());
    }
  }

  // query
  wwsearch::Searcher searcher(&index->Config());
  std::list<DocumentID> match_documentsid;

  //
  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "old");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
  }
  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "value");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
  }
  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "new");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
  }
  {
    match_documentsid.clear();
    uint32_t value = 1;
    wwsearch::BooleanQuery query1(2, value);
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
  }
  {
    match_documentsid.clear();
    uint32_t value = 2;
    wwsearch::BooleanQuery query1(2, value);
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
  }
  {
    match_documentsid.clear();
    // uint32_t value = 2;
    wwsearch::BooleanQuery query1(5, "add");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
  }
  {
    match_documentsid.clear();
    uint32_t value = 20;
    wwsearch::BooleanQuery query1(6, value);
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
  }
}

}  // namespace wwsearch
