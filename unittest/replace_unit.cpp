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

class IndexReplaceTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;
  std::list<DocumentID> match_documentsid;

 public:
  IndexReplaceTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path = std::string("/tmp/unit_") + std::string("replace");
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

DefaultIndexWrapper *IndexReplaceTest::index = nullptr;
DocumentID IndexReplaceTest::document_id = 1;

TEST_F(IndexReplaceTest, AddAndReplaceDocument) {
  DocumentID document_id = GetDocumentID();
  auto du = TestUtil::NewDocument(document_id, "abc", 1, 1, 2);
  {
    documents.push_back(du);
    bool ret =
        index->index_writer_->AddDocuments(table, documents, nullptr, nullptr);
    EXPECT_TRUE(ret);  // document not exist
  }

  du = TestUtil::NewDocument(document_id, "efg", 1, 1, 2, 5);
  {
    for (auto document : documents) delete document;
    documents.clear();
    documents.push_back(du);
    bool ret = index->index_writer_->ReplaceDocuments(table, documents, nullptr,
                                                      nullptr);
    EXPECT_TRUE(ret);
  }

  wwsearch::Searcher searcher(&index->Config());
  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "abc");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
  }
  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(6, "efg");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
  }
}

}  // namespace wwsearch
