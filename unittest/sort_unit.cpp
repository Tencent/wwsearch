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
#include "include/collector_top.h"
#include "include/document.h"
#include "include/index_wrapper.h"
#include "include/search_util.h"
#include "include/sorter.h"
#include "unittest_util.h"

extern bool g_debug;
extern bool g_use_rocksdb;
extern bool g_use_compression;

namespace wwsearch {

class SortTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper* index;
  static uint64_t document_id;
  static uint64_t numeric_value;
  wwsearch::TableID table;
  std::vector<DocumentUpdater*> documents;
  std::list<DocumentID> match_documentsid;

 public:
  SortTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path = std::string("/tmp/unit_") + std::string("sort");
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

  virtual void SetUp() override {
    table.partition_set++;
    match_documentsid.clear();
  }

  virtual void TearDown() override {
    for (auto du : documents) {
      delete du;
    }
    documents.clear();
    match_documentsid.clear();
  }

  uint64_t GetDocumentID() { return document_id++; }

  uint64_t GetNumeric(uint64_t alloc_len = 1000) {
    auto temp = numeric_value;
    numeric_value += alloc_len;
    return temp;
  }

 private:
};

DefaultIndexWrapper* SortTest::index = nullptr;
DocumentID SortTest::document_id = 1;
DocumentID SortTest::numeric_value = 1;

TEST_F(SortTest, Filters) {
  auto base = GetNumeric(10000);
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloa", base,
                                            base + 100, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloa", base + 1,
                                            base + 101, base + 70));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloa", base + 2,
                                            base + 102, base + 71));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  if (g_debug) {
    for (const auto& du : documents) {
      EXPECT_EQ(0, du->Status().GetCode());
      wwsearch::Document& document = du->New();
      std::string debug_str;
      document.PrintToReadStr(debug_str);
      SearchLogDebug("%s\n", debug_str.c_str());
    }
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    std::vector<wwsearch::SortCondition*> store_sort;
    {
      wwsearch::SortCondition* sc =
          new wwsearch::NumericSortCondition(4, ::wwsearch::kSortConditionDesc);
      store_sort.push_back(sc);
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, &store_sort,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    ASSERT_EQ(3, match_documentsid.size());

    bool first = true;
    DocumentID prev;
    for (auto id : match_documentsid) {
      if (first) {
        first = false;
        prev = id;
      } else {
        EXPECT_TRUE(prev >= id);
        prev = id;
      }
    }
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    match_documentsid.clear();
    std::vector<wwsearch::SortCondition*> store_sort;
    {
      wwsearch::SortCondition* sc =
          new wwsearch::NumericSortCondition(4, ::wwsearch::kSortConditionAsc);
      store_sort.push_back(sc);
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, &store_sort,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    ASSERT_EQ(3, match_documentsid.size());

    bool first = true;
    DocumentID prev;
    for (auto id : match_documentsid) {
      if (first) {
        first = false;
        prev = id;
      } else {
        EXPECT_TRUE(prev <= id);
        prev = id;
      }
    }
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  // default sort
  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    ASSERT_EQ(3, match_documentsid.size());

    bool first = true;
    DocumentID prev;
    for (auto id : match_documentsid) {
      if (first) {
        first = false;
        prev = id;
      } else {
        EXPECT_TRUE(prev > id);
        prev = id;
      }
    }
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
}

TEST_F(SortTest, DefaultSort) {
  Document doc1;
  doc1.SetID(101);

  Document doc2;
  doc2.SetID(102);

  Document doc3;
  doc3.SetID(103);

  PriorityQueue pri_queue(Sorter(nullptr));
  pri_queue.push(&doc2);
  pri_queue.push(&doc1);
  pri_queue.push(&doc3);

  EXPECT_EQ(pri_queue.top()->ID(), doc1.ID());
  pri_queue.pop();

  EXPECT_EQ(pri_queue.top()->ID(), doc2.ID());
  pri_queue.pop();

  EXPECT_EQ(pri_queue.top()->ID(), doc3.ID());
  pri_queue.pop();

  EXPECT_EQ(pri_queue.size(), 0);
}

TEST_F(SortTest, StringSort) {
  auto base = GetNumeric(10000);
  documents.clear();
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "aacc", base,
                                            base + 100, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "aa", base,
                                            base + 101, base + 70));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "cc", base,
                                            base + 102, base + 71));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  if (g_debug) {
    for (const auto& du : documents) {
      EXPECT_EQ(0, du->Status().GetCode());
      wwsearch::Document& document = du->New();
      std::string debug_str;
      document.PrintToReadStr(debug_str);
      SearchLogDebug("%s\n", debug_str.c_str());
    }
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    std::vector<wwsearch::SortCondition*> store_sort;
    {
      wwsearch::SortCondition* sc =
          new wwsearch::StringSortCondition(1, ::wwsearch::kSortConditionAsc);
      store_sort.push_back(sc);
    }

    wwsearch::BooleanQuery query1(2, (uint32_t)base);
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, &store_sort,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    ASSERT_EQ(3, match_documentsid.size());

    std::vector<uint64_t> ids;
    for (auto id : match_documentsid) {
      ids.push_back(id);
      printf("id %lu\n", id);
    }
    ASSERT_TRUE(ids[1] < ids[0]);
    ASSERT_TRUE(ids[0] < ids[2]);

    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
}

}  // namespace wwsearch
