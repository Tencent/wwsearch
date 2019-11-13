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
#include "include/search_util.h"
#include "unittest_util.h"

extern bool g_debug;
extern bool g_use_rocksdb;
extern bool g_use_compression;

namespace wwsearch {

class FilterTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper* index;
  static uint64_t document_id;
  static uint64_t numeric_value;
  wwsearch::TableID table;
  std::vector<DocumentUpdater*> documents;
  std::list<DocumentID> match_documentsid;

 public:
  FilterTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path = std::string("/tmp/unit_") + std::string("filter");
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

DefaultIndexWrapper* FilterTest::index = nullptr;
DocumentID FilterTest::document_id = 1;
DocumentID FilterTest::numeric_value = 1;

TEST_F(FilterTest, Filters) {
  auto base = GetNumeric(10000);
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloa", base,
                                            base + 100, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloa", base + 1,
                                            base + 101, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloa", base + 2,
                                            base + 102, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "stringfilter",
                                            base + 3, base + 102, base + 69));

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
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::EqualFilter* filter = new wwsearch::EqualFilter();
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(3, wwsearch::IndexFieldFlag());
      field->SetUint64(base + 101);
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
  // string filter
  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::EqualFilter* filter = new wwsearch::EqualFilter();
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(1, wwsearch::IndexFieldFlag());
      field->SetString("stringfilter");
    }

    wwsearch::BooleanQuery query1(1, "stringfilter");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::NotEqualFilter* filter = new wwsearch::NotEqualFilter();
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(1, wwsearch::IndexFieldFlag());
      field->SetString("stringfilter");
    }

    wwsearch::BooleanQuery query1(1, "stringfilter");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    if (g_debug) {
    }
  }
  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::NotEqualFilter* filter = new wwsearch::NotEqualFilter();
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(3, wwsearch::IndexFieldFlag());
      field->SetUint64(base + 101);
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::RangeFilter* filter =
          new wwsearch::RangeFilter(base + 101, base + 102);
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(3, wwsearch::IndexFieldFlag());
      // ATTATION!!! NEED TO SET THIS TO MAKE SURE IT NUMERIC
      field->SetUint64(base + 101);
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::RangeFilter* filter =
          new wwsearch::RangeFilter(base + 101, base + 102);
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(99 /*not exist field id*/, wwsearch::IndexFieldFlag());
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::InFilter* filter = new wwsearch::InFilter;
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(1, wwsearch::IndexFieldFlag());
      field->SetString("ello");
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(3, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::NotInFilter* filter = new wwsearch::NotInFilter;
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(1, wwsearch::IndexFieldFlag());
      field->SetString("ello");
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      wwsearch::NotInFilter* filter = new wwsearch::NotInFilter;
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(1, wwsearch::IndexFieldFlag());
      field->SetString("xibaocao");
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(3, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      std::vector<uint64_t> numeric_list{base + 101, base + 10001,
                                         base + 10002};
      wwsearch::InNumericListFilter* filter =
          new wwsearch::InNumericListFilter();
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(3, wwsearch::IndexFieldFlag());
      field->SetNumericList(numeric_list);
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    match_documentsid.clear();
    std::vector<wwsearch::Filter*> store_filter;
    {
      std::vector<uint64_t> numeric_list{base + 101, base + 10001,
                                         base + 10002};
      wwsearch::NotInNumericListFilter* filter =
          new wwsearch::NotInNumericListFilter();
      store_filter.push_back(filter);
      wwsearch::IndexField* field = filter->GetField();
      field->SetMeta(3, wwsearch::IndexFieldFlag());
      field->SetNumericList(numeric_list);
    }

    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, &store_filter,
                                   nullptr, match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
}

}  // namespace wwsearch
