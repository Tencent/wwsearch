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
#include "include/prefix_query.h"
#include "include/search_util.h"
#include "unittest_util.h"

extern bool g_debug;
extern bool g_use_rocksdb;
extern bool g_use_compression;

namespace wwsearch {

class BoolQueryTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  static uint64_t numeric_value;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;
  std::list<DocumentID> match_documentsid;

 public:
  BoolQueryTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path =
        std::string("/tmp/unit_") + std::string("boolquery");
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

DefaultIndexWrapper *BoolQueryTest::index = nullptr;
DocumentID BoolQueryTest::document_id = 1;
DocumentID BoolQueryTest::numeric_value = 1;

TEST_F(BoolQueryTest, Query_String) {
  auto base = GetNumeric(10000);
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloa", base,
                                            base + 100, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "girla", base + 1,
                                            base + 101, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloa", base + 2,
                                            base + 102, base + 69));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  if (g_debug) {
    for (const auto &du : documents) {
      EXPECT_EQ(0, du->Status().GetCode());
      wwsearch::Document &document = du->New();
      std::string debug_str;
      document.PrintToReadStr(debug_str);
      SearchLogDebug("%s\n", debug_str.c_str());
    }
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "helloa");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
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
    wwsearch::BooleanQuery query2(1, "girla");
    auto status = searcher.DoQuery(table, query2, 0, 100, nullptr, nullptr,
                                   match_documentsid);
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
    wwsearch::BooleanQuery query3(1, "nothing1");
    auto status = searcher.DoQuery(table, query3, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
}

TEST_F(BoolQueryTest, Query_SuffixString) {
  auto base = GetNumeric(10000);
  auto document_updater = TestUtil::NewDocument(
      GetDocumentID(), "sufixxxxxxxxx", base, base + 100, base + 69);
  document_updater->New().FindField(1)->Flag().SetSuffixBuild();
  documents.push_back(document_updater);
  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  if (g_debug) {
    for (const auto &du : documents) {
      EXPECT_EQ(0, du->Status().GetCode());
      wwsearch::Document &document = du->New();
      std::string debug_str;
      document.PrintToReadStr(debug_str);
      SearchLogDebug("%s\n", debug_str.c_str());
    }
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "fixxxxxxxxx");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
}

TEST_F(BoolQueryTest, PrefixQuery) {
  auto base = GetNumeric(10000);
  {
    auto document_updater = TestUtil::NewDocument(GetDocumentID(), "prefixxxxx",
                                                  base, base + 100, base + 69);
    document_updater->New().FindField(1)->Flag().SetSuffixBuild();
    documents.push_back(document_updater);
  }
  {
    auto document_updater = TestUtil::NewDocument(
        GetDocumentID(), "wonderrefixg", base, base + 100, base + 69);
    document_updater->New().FindField(1)->Flag().SetSuffixBuild();
    documents.push_back(document_updater);
  }

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  if (g_debug) {
    for (const auto &du : documents) {
      EXPECT_EQ(0, du->Status().GetCode());
      wwsearch::Document &document = du->New();
      std::string debug_str;
      document.PrintToReadStr(debug_str);
      SearchLogDebug("%s\n", debug_str.c_str());
    }
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    wwsearch::PrefixQuery query1(1, "refix");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
}

TEST_F(BoolQueryTest, Query_Chinese) {
  auto base = GetNumeric(10000);
  auto document_updater = TestUtil::NewDocument(
      GetDocumentID(),
      "我是中国人，我爱中国。为了加速倒排列表归并。归并过程是按DocumentID从大到"
      "小的顺序遍历的，在遍历时要用一个堆结构来加速查找当前最大的DocumentID属于"
      "哪个列表。这个堆结构用内存池分配。",
      base, base + 100, base + 69);
  // document_updater->New().FindField(1)->Flag().SetSuffixBuild();
  documents.push_back(document_updater);
  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  if (g_debug) {
    for (const auto &du : documents) {
      EXPECT_EQ(0, du->Status().GetCode());
      wwsearch::Document &document = du->New();
      std::string debug_str;
      document.PrintToReadStr(debug_str);
      SearchLogDebug("%s\n", debug_str.c_str());
    }
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "分配");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
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
    wwsearch::BooleanQuery query1(1, "不存在");
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
}

TEST_F(BoolQueryTest, Query_Numeric) {
  auto base = GetNumeric(10000);
  documents.push_back(
      TestUtil::NewDocument(GetDocumentID(), "hellob", base, base, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "girlb", base + 1,
                                            base, base + 69));
  documents.push_back(
      TestUtil::NewDocument(GetDocumentID(), "hellob", base, base, base + 69));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }

  wwsearch::Searcher searcher(&index->Config());

  uint32_t value;
  {
    match_documentsid.clear();
    value = base;
    wwsearch::BooleanQuery query1(2, value);
    auto status = searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
  }

  {
    match_documentsid.clear();
    value = base + 1;
    wwsearch::BooleanQuery query2(2, value);
    auto status = searcher.DoQuery(table, query2, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
  }

  {
    match_documentsid.clear();
    value = 0;
    wwsearch::BooleanQuery query3(2, value);
    auto status = searcher.DoQuery(table, query3, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
  }
}

TEST_F(BoolQueryTest, TextQueryUsingTokenize) {
  auto base = GetNumeric(10000);
  const std::string doc_text1{
      " \303\244\302\270\302\245\303\245\302\276\302\267\303\250\305\222\342"
      "\200\232 "
      "\303\245\302\267\302\245\303\244\302\275\305\223\303\244\302\273\302\273"
      "\303\245\305\240\302\241\303\245\302\215\342\200\242 "
      "\303\244\302\270\302\245\303\245\302\276\302\267\303\250\305\222\342\200"
      "\232\303\247\305\241\342\200\236\303\245\302\267\302\245\303\244\302\275"
      "\305\223\303\244\302\273\302\273\303\245\305\240\302\241\303\245\302\215"
      "\342\200\242 "
      "\303\244\302\270\302\245\303\245\302\276\302\267\303\250\305\222\342\200"
      "\232\'s\303\245\302\267\302\245\303\244\302\275\305\223\303\244\302\273"
      "\302\273\303\245\305\240\302\241\303\245\302\215\342\200\242  "
      "\303\251\302\241\302\271\303\247\342\200\272\302\256\303\245\302\220\302"
      "\215\303\247\302\247\302\260 "
      "971\303\245\302\261\342\200\242\303\250\302\275\302\246 "
      "\303\251\342\200\241\342\200\241\303\250\302\264\302\255\303\246\313\234"
      "\305\275\303\247\302\273\342\200\240 "
      "\303\245\302\220\313\206\303\245\302\220\305\222\303\245\302\217\302\267"
      " HT2018-12-020 "
      "\303\244\302\270\342\200\271\303\245\302\215\342\200\242\303\246\342\200"
      "\224\302\245\303\246\305\223\305\270 "
      "\303\244\302\270\342\200\271\303\245\302\215\342\200\242\303\251\306\222"
      "\302\250\303\251\342\200\224\302\250 "
      "\303\245\302\220\305\275\303\245\302\244\342\200\236\303\247\302\220\342"
      "\200\240\303\246\302\235\305\275\303\244\302\277\305\240\303\246\302\235"
      "\302\260 "
      "\303\245\302\247\342\200\235\303\246\302\264\302\276\303\251\306\222\302"
      "\250\303\251\342\200\224\302\250 "
      "\303\244\302\273\342\200\234\303\245\302\272\342\200\234\303\244\302\270"
      "\342\200\271\303\246\342\200\223\342\204\242\303\245\305\222\302\272 "
      "\303\251\305\223\342\202\254\303\246\302\261\342\200\232\303\244\302\272"
      "\302\272\303\246\342\200\242\302\260 2 "
      "\303\244\302\272\302\272\303\245\342\200\230\313\234\303\245\302\247\342"
      "\200\234\303\245\302\220\302\215  "
      "\303\245\342\200\241\302\272\303\245\302\267\302\256\303\245\305\223\302"
      "\260\303\245\302\235\342\202\254  "
      "\303\245\302\256\302\242\303\246\313\206\302\267\303\250\302\201\342\200"
      "\235\303\247\302\263\302\273\303\244\302\272\302\272\303\245\302\217\305"
      "\240\303\247\342\200\235\302\265\303\250\302\257\302\235  "
      "\303\250\302\246\302\201\303\246\302\261\342\200\232\303\245\342\200\241"
      "\302\272\303\245\302\217\342\200\230\303\246\342\200\224\302\266\303\251"
      "\342\200\224\302\264 "
      "\303\251\302\241\302\271\303\247\342\200\272\302\256\303\247\302\273\302"
      "\217\303\247\302\220\342\200\240\303\245\302\247\342\200\234\303\245\302"
      "\220\302\215\303\250\302\201\342\200\235\303\247\302\263\302\273\303\246"
      "\342\200\223\302\271\303\245\302\274\302\217  "
      "\303\250\302\246\302\201\303\246\302\261\342\200\232\303\245\302\256\305"
      "\222\303\246\313\206\302\220\303\246\342\200\224\302\266\303\251\342\200"
      "\224\302\264 "
      "\303\244\302\273\302\273\303\245\305\240\302\241\303\245\342\200\240\342"
      "\200\246\303\245\302\256\302\271 "
      "12mm\303\246\305\223\302\250\303\246\302\235\302\277\303\257\302\274\305"
      "\241\n410*440*2\303\245\302\235\342\200\224\n360*440*"
      "2\303\245\302\235\342\200\224\n220*440*"
      "4\303\245\302\235\342\200\224\n460*160*300*"
      "2\n\303\245\302\244\302\215\303\246\302\250\302\241\303\247\342\200\235"
      "\302\250\303\245\302\220\305\275\303\245\302\244\342\200\236\303\247\302"
      "\220\342\200\240\303\251\302\242\342\200\2403/"
      "11\303\244\302\270\342\200\271\303\245\302\215\313\2067\303\247\342\200"
      "\232\302\271\303\245\302\215\305\240\303\245\342\200\260\302\215\303\245"
      "\302\256\305\222\303\246\313\206\302\220 "
      "\303\244\302\273\302\273\303\245\305\240\302\241\303\244\302\270\342\200"
      "\271\303\250\302\276\302\276\303\244\302\272\302\272  "
      "\303\250\302\257\302\264\303\246\313\234\305\275"};
  const std::string doc_text2{
      " \303\251\342\200\241\342\200\230\303\246\342\202\254\302\235\303\247"
      "\342\200\236\302\261 "
      "\303\245\302\247\342\200\235\303\245\302\244\342\200\223\303\245\305\240"
      "\302\240\303\245\302\267\302\245\303\247\342\200\235\302\263\303\250\302"
      "\257\302\267\303\245\302\215\342\200\242 "
      "\303\251\342\200\241\342\200\230\303\246\342\202\254\302\235\303\247\342"
      "\200\236\302\261\303\247\305\241\342\200\236\303\245\302\247\342\200\235"
      "\303\245\302\244\342\200\223\303\245\305\240\302\240\303\245\302\267\302"
      "\245\303\247\342\200\235\302\263\303\250\302\257\302\267\303\245\302\215"
      "\342\200\242 "
      "\303\251\342\200\241\342\200\230\303\246\342\202\254\302\235\303\247\342"
      "\200\236\302\261\'s\303\245\302\247\342\200\235\303\245\302\244\342\200"
      "\223\303\245\305\240\302\240\303\245\302\267\302\245\303\247\342\200\235"
      "\302\263\303\250\302\257\302\267\303\245\302\215\342\200\242  "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\245\342\200\246"
      "\302\254\303\245\302\217\302\270\303\245\302\220\302\215\303\247\302\247"
      "\302\260 "
      "\303\246\302\265\302\267\303\247\302\273\302\264\303\246\342\200\223\302"
      "\257\303\247\342\200\260\302\271\303\246\302\261\302\275\303\250\302\275"
      "\302\246\303\245\302\267\302\245\303\247\302\250\342\200\271 "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\251\306\222\302"
      "\250\303\251\342\200\224\302\250 "
      "\303\247\342\200\235\305\270\303\244\302\272\302\247\303\250\302\256\302"
      "\241\303\245\313\206\342\200\231 "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\246\342\200\224"
      "\302\245\303\246\305\223\305\270 "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\245\302\215\342"
      "\200\242\303\245\302\217\302\267 "
      "\303\251\342\200\241\342\200\241\303\250\302\264\302\255\303\246\313\234"
      "\305\275\303\247\302\273\342\200\240 "
      "\303\247\342\200\260\302\251\303\245\342\200\234\302\201\303\245\302\220"
      "\302\215\303\247\302\247\302\260 "
      "\303\245\342\200\260\302\215\303\245\302\220\305\275\303\251\342\200\224"
      "\302\250\303\251\342\204\242\302\220\303\244\302\275\302\215\303\245\302"
      "\274\342\202\254\303\245\342\200\246\302\263 "
      "\303\246\342\200\242\302\260\303\251\342\200\241\302\217 "
      "\303\245\313\206\302\260\303\250\302\264\302\247\303\246\342\200\224\302"
      "\266\303\251\342\200\224\302\264 "
      "\303\245\302\220\313\206\303\245\302\220\305\222\303\245\302\217\302\267"
      " HT2018-12-020 "
      "\303\245\305\240\302\240\303\245\302\267\302\245\303\250\302\246\302\201"
      "\303\246\302\261\342\200\232 "
      "\303\246\305\222\342\200\260\303\246\342\200\242\302\260\303\246\302\215"
      "\302\256\303\245\305\240\302\240\303\245\302\267\302\245\303\245\313\206"
      "\302\266\303\244\302\275\305\223\n\\\\192.168.1.2\\public2\\2018."
      "12\\HT2018-12-"
      "020971\303\246\302\246\342\200\232\303\245\302\277\302\265\303\250\302"
      "\275\302\246\\\303\245\305\240\302\240\303\245\302\267\302\245\303\246"
      "\342\200\242\302\260\303\246\302\215\302\256\\SC\\SH190311-11-"
      "971XianWeiKaiGuanDiZuoJiaQiangSeCaiDingYi\303\257\302\274\313\2062019031"
      "1\303\257\302\274\342\200\260 "
      "\303\245\302\244\342\200\241\303\246\302\263\302\250 "
      "\303\246\302\263\302\250\303\257\302\274\305\2412."
      "5\303\247\305\241\342\200\236\303\245\302\255\342\200\235\303\246\342"
      "\200\235\302\273M3\303\247\305\241\342\200\236\303\247\342\200\260\342"
      "\204\242\303\257\302\274\305\222\303\251\302\242\342\200\236\303\247\302"
      "\256\342\200\224\303\245\302\217\302\267\303\257\302\274\305\241YS-XM-"
      "201812-020 \303\251\342\204\242\342\200\236\303\244\302\273\302\266"};
  const std::string doc_text3{
      " \303\250\306\222\302\241\303\244\302\270\342\200\223\303\246\302\235"
      "\302\260 "
      "\303\246\305\240\342\202\254\303\246\305\223\302\257\303\245\302\267\302"
      "\245\303\247\302\250\342\200\271\303\251\306\222\302\250\303\251\342\200"
      "\241\342\200\241\303\250\302\264\302\255\303\247\342\200\235\302\263\303"
      "\250\302\257\302\267 "
      "\303\250\306\222\302\241\303\244\302\270\342\200\223\303\246\302\235\302"
      "\260\303\247\305\241\342\200\236\303\246\305\240\342\202\254\303\246\305"
      "\223\302\257\303\245\302\267\302\245\303\247\302\250\342\200\271\303\251"
      "\306\222\302\250\303\251\342\200\241\342\200\241\303\250\302\264\302\255"
      "\303\247\342\200\235\302\263\303\250\302\257\302\267 "
      "\303\250\306\222\302\241\303\244\302\270\342\200\223\303\246\302\235\302"
      "\260\'s\303\246\305\240\342\202\254\303\246\305\223\302\257\303\245\302"
      "\267\302\245\303\247\302\250\342\200\271\303\251\306\222\302\250\303\251"
      "\342\200\241\342\200\241\303\250\302\264\302\255\303\247\342\200\235\302"
      "\263\303\250\302\257\302\267  "
      "\303\251\305\223\342\202\254\303\246\302\261\342\200\232\303\251\306\222"
      "\302\250\303\251\342\200\224\302\250 "
      "\303\245\302\267\302\245\303\247\302\250\342\200\271\303\251\306\222\302"
      "\250 "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\246\342\200\224"
      "\302\245\303\246\305\223\305\270 "
      "\303\251\342\200\241\342\200\241\303\250\302\264\302\255\303\246\313\234"
      "\305\275\303\247\302\273\342\200\240 "
      "\303\251\302\242\342\200\236\303\247\302\256\342\200\224\303\245\302\215"
      "\342\200\242\303\245\302\217\302\267 YS-XM-201812-020 "
      "\303\245\305\276\342\200\271\303\245\302\217\302\267\303\246\313\206\342"
      "\200\223\303\250\302\247\342\200\236\303\246\302\240\302\274 "
      "\303\246\342\204\242\302\256\303\251\342\202\254\305\241\303\247\302\201"
      "\302\257\303\245\302\270\302\246\303\251\342\200\241\342\200\241\303\250"
      "\302\264\302\255 "
      "\303\245\302\220\313\206\303\245\302\220\305\222\303\245\302\217\302\267"
      " HT-2018-12-020 "
      "\303\246\342\200\242\302\260\303\251\342\200\241\302\217 "
      "\303\245\302\215\342\200\242\303\244\302\273\302\267 "
      "\303\251\342\200\241\342\200\230\303\251\302\242\302\235 "
      "\303\247\342\200\235\302\250\303\251\342\202\254\342\200\235 "
      "971\303\246\342\204\242\302\256\303\251\342\202\254\305\241\303\247\302"
      "\201\302\257\303\245\302\270\302\246\303\251\342\200\241\342\200\241\303"
      "\250\302\264\302\255\303\244\302\270\342\200\271\303\245\302\215\342\200"
      "\242 "
      "\303\245\313\206\302\260\303\250\302\264\302\247\303\246\342\200\224\302"
      "\245\303\246\305\223\305\270 "
      "\303\245\302\272\342\200\234\303\245\302\255\313\234\303\246\342\200\242"
      "\302\260\303\251\342\200\241\302\217 "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\244\302\272\302"
      "\272 "
      "\303\250\306\222\302\241\303\244\302\270\342\200\223\303\246\302\235\302"
      "\260 \303\251\342\204\242\342\200\236\303\244\302\273\302\266"};
  std::string doc_text4{
      " \303\247\305\275\342\200\271\303\250\342\200\271\342\200\224\303\250"
      "\342\200\271\342\200\224 "
      "\303\245\302\247\342\200\235\303\245\302\244\342\200\223\303\245\305\240"
      "\302\240\303\245\302\267\302\245\303\247\342\200\235\302\263\303\250\302"
      "\257\302\267\303\245\302\215\342\200\242 "
      "\303\247\305\275\342\200\271\303\250\342\200\271\342\200\224\303\250\342"
      "\200\271\342\200\224\303\247\305\241\342\200\236\303\245\302\247\342\200"
      "\235\303\245\302\244\342\200\223\303\245\305\240\302\240\303\245\302\267"
      "\302\245\303\247\342\200\235\302\263\303\250\302\257\302\267\303\245\302"
      "\215\342\200\242 "
      "\303\247\305\275\342\200\271\303\250\342\200\271\342\200\224\303\250\342"
      "\200\271\342\200\224\'s\303\245\302\247\342\200\235\303\245\302\244\342"
      "\200\223\303\245\305\240\302\240\303\245\302\267\302\245\303\247\342\200"
      "\235\302\263\303\250\302\257\302\267\303\245\302\215\342\200\242  "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\245\342\200\246"
      "\302\254\303\245\302\217\302\270\303\245\302\220\302\215\303\247\302\247"
      "\302\260 "
      "\303\246\302\265\302\267\303\247\302\273\302\264\303\246\342\200\223\302"
      "\257\303\247\342\200\260\302\271\303\246\302\261\302\275\303\250\302\275"
      "\302\246\303\245\302\267\302\245\303\247\302\250\342\200\271 "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\251\306\222\302"
      "\250\303\251\342\200\224\302\250 "
      "\303\251\342\200\241\342\200\241\303\250\302\264\302\255 "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\246\342\200\224"
      "\302\245\303\246\305\223\305\270 "
      "\303\247\342\200\235\302\263\303\250\302\257\302\267\303\245\302\215\342"
      "\200\242\303\245\302\217\302\267 "
      "\303\251\342\200\241\342\200\241\303\250\302\264\302\255\303\246\313\234"
      "\305\275\303\247\302\273\342\200\240 "
      "\303\247\342\200\260\302\251\303\245\342\200\234\302\201\303\245\302\220"
      "\302\215\303\247\302\247\302\260 "
      "\303\251\302\241\302\266\303\246\302\243\305\241\303\246\305\223\302\272"
      "\303\245\342\204\242\302\250\303\244\302\272\302\272VVC\303\246\302\235"
      "\302\220\303\246\342\200\223\342\204\242 "
      "\303\246\342\200\242\302\260\303\251\342\200\241\302\217 "
      "\303\245\313\206\302\260\303\250\302\264\302\247\303\246\342\200\224\302"
      "\266\303\251\342\200\224\302\264 "
      "\303\245\302\220\313\206\303\245\302\220\305\222\303\245\302\217\302\267"
      " HT2018-12-020 "
      "\303\245\305\240\302\240\303\245\302\267\302\245\303\250\302\246\302\201"
      "\303\246\302\261\342\200\232  "
      "\303\245\302\244\342\200\241\303\246\302\263\302\250 "
      "HT2018-12-"
      "020971\303\246\302\246\342\200\232\303\245\302\277\302\265\303\250\302"
      "\275\302\246-"
      "\303\251\302\241\302\266\303\246\302\243\305\241\303\246\305\223\302\272"
      "\303\245\342\204\242\302\250\303\244\302\272\302\272-"
      "\303\251\302\273\342\200\230\303\250\342\200\260\302\262VVC\303\246\302"
      "\235\302\220\303\246\342\200\223\342\204\242-"
      "\303\245\342\200\246\302\261\303\250\302\256\302\2415\303\244\302\273"
      "\302\266\303\243\342\202\254\342\200\232\303\244\302\276\342\200\272\303"
      "\245\302\272\342\200\235\303\245\342\200\242\342\200\240\303\245\302\263"
      "\302\273\303\245\302\256\302\270\303\246\305\240\302\245\303\244\302\273"
      "\302\267190\303\245\342\200\246\306\222\303\257\302\274\313\2063.9/"
      "\303\245\342\200\246\342\200\271\303\257\302\274\342\200\260\303\257\302"
      "\274\305\222\303\244\302\272\302\244\303\246\305\223\305\2703/"
      "13\303\245\302\217\302\267\303\243\342\202\254\342\200\232\303\245\302"
      "\217\342\200\230\303\245\302\263\302\273\303\245\302\256\302\270\303\257"
      "\302\274\305\222\303\250\302\257\302\267\303\251\302\242\342\200\240\303"
      "\245\302\257\302\274\303\245\342\200\231\305\222\303\251\342\204\242\313"
      "\206\303\246\342\202\254\302\273\303\246\342\200\260\302\271\303\245\342"
      "\200\241\342\200\240\303\257\302\274\305\222\303\250\302\260\302\242\303"
      "\250\302\260\302\242\303\257\302\274\302\201 "
      "\303\251\342\204\242\342\200\236\303\244\302\273\302\266"};
  std::string doc_text5{
      " \303\251\342\204\242\313\206\303\250\305\275\342\200\260 "
      "\303\246\305\240\302\245\303\245\302\272\305\270\303\245\302\215\342\200"
      "\242 "
      "\303\251\342\204\242\313\206\303\250\305\275\342\200\260\303\247\305\241"
      "\342\200\236\303\246\305\240\302\245\303\245\302\272\305\270\303\245\302"
      "\215\342\200\242 "
      "\303\251\342\204\242\313\206\303\250\305\275\342\200\260\'s\303\246\305"
      "\240\302\245\303\245\302\272\305\270\303\245\302\215\342\200\242  "
      "NO\303\247\302\274\342\200\223\303\245\302\217\302\267 HVBF2019-0312 "
      "\303\245\302\220\313\206\303\245\302\220\305\222\303\245\302\217\302\267"
      " HT2018-12-"
      "020971\303\246\302\246\342\200\232\303\245\302\277\302\265\303\250\302"
      "\275\302\246 "
      "\303\246\305\240\302\245\303\245\302\272\305\270\303\245\342\200\240\342"
      "\200\246\303\245\302\256\302\271\303\245\302\217\305\240\303\245\305\275"
      "\305\270\303\245\342\200\272\302\240 "
      "\303\245\302\267\302\246\303\245\342\200\260\302\215\303\251\342\200\224"
      "\302\250\303\251\302\245\302\260\303\246\302\235\302\241\303\257\302\274"
      "\313\2061106X129X91\303\257\302\274\342\200\260\303\257\302\274\342\200"
      "\272\303\245\302\217\302\263\303\245\342\200\260\302\215\303\251\342\200"
      "\224\302\250\303\251\302\245\302\260\303\246\302\235\302\241\303\257\302"
      "\274\313\2061106X129X91\303\257\302\274\342\200\260\303\257\302\274\342"
      "\200\272\n\303\245\342\200\272\302\240\303\246\305\223\302\252\303\247"
      "\342\200\242\342\204\242\303\245\305\222\302\271\303\251\342\200\246\302"
      "\215\303\244\302\275\342\204\242\303\251\342\200\241\302\217\303\257\302"
      "\274\305\222\303\245\302\256\305\276\303\251\342\204\242\342\200\246\303"
      "\250\302\243\342\200\246\303\251\342\200\246\302\215\303\246\302\257\342"
      "\200\235\303\247\305\275\302\273\303\247\342\200\231\306\222\303\247\305"
      "\270\302\255\303\244\302\272\342\200\240\303\244\302\270\342\202\254\303"
      "\247\342\200\232\302\271\303\257\302\274\305\222\303\251\305\223\342\202"
      "\254\303\246\305\240\302\245\303\245\302\272\305\270\303\251\342\200\241"
      "\302\215\303\246\342\200\223\302\260\303\245\313\206\302\266\303\244\302"
      "\275\305\223\303\243\342\202\254\342\200\232 "
      "\303\251\342\200\272\302\266\303\244\302\273\302\266\303\247\302\274\342"
      "\200\223\303\245\302\217\302\267 201812020G0061-001201812020G0061-002 "
      "\303\250\302\264\302\243\303\244\302\273\302\273\303\244\302\272\302\272"
      " \303\247\305\275\342\200\271\303\251\342\200\241\342\200\230\303\245"
      "\302\272\342\200\240 "
      "\303\251\306\222\302\250\303\251\342\200\224\302\250 "
      "\303\245\302\267\302\245\303\247\302\250\342\200\271 "
      "\303\245\302\241\302\253\303\245\302\215\342\200\242\303\244\302\272\302"
      "\272 "
      "\303\247\305\275\342\200\271\303\251\342\200\241\342\200\230\303\245\302"
      "\272\342\200\240 "
      "\303\245\302\241\302\253\303\245\302\215\342\200\242\303\246\342\200\224"
      "\302\266\303\251\342\200\224\302\264 "
      "\303\246\305\275\302\245\303\245\302\215\342\200\242\303\251\306\222\302"
      "\250\303\251\342\200\224\302\250\303\250\302\264\305\270\303\250\302\264"
      "\302\243\303\244\302\272\302\272  "
      "\303\251\342\200\241\302\215\303\246\342\200\223\302\260\303\245\313\206"
      "\302\266\303\244\302\275\305\223\303\247\302\241\302\256\303\250\302\256"
      "\302\244  "
      "\303\247\302\241\302\256\303\250\302\256\302\244\303\244\302\272\302\272"
      "  "
      "\303\247\302\241\302\256\303\250\302\256\302\244\303\246\342\200\224\302"
      "\266\303\251\342\200\224\302\264 "
      "\303\246\342\200\260\342\202\254\303\251\305\223\342\202\254\303\246\302"
      "\235\302\220\303\246\342\200\223\342\204\242  "
      "\303\246\342\200\260\342\202\254\303\251\305\223\342\202\254\303\246\302"
      "\235\302\220\303\246\342\200\223\342\204\242\303\246\342\200\224\302\266"
      "\303\251\342\200\224\302\264 "
      "\303\246\302\235\302\220\303\246\342\200\223\342\204\242\303\250\302\264"
      "\302\271  "
      "\303\244\302\272\302\272\303\245\302\267\302\245\303\250\302\264\302\271"
      "  "
      "\303\245\302\220\313\206\303\250\302\256\302\241\303\250\302\264\302\271"
      "\303\247\342\200\235\302\250  "
      "\303\245\302\256\305\222\303\246\313\206\302\220\303\246\342\200\224\302"
      "\266\303\251\342\200\224\302\264 "
      "\303\245\302\256\305\222\303\246\313\206\302\220\303\246\306\222\342\200"
      "\246\303\245\342\200\240\302\265  "
      "\303\250\302\264\302\250\303\246\302\243\342\202\254\303\247\302\255\302"
      "\276\303\245\302\255\342\200\224\303\247\302\241\302\256\303\250\302\256"
      "\302\244  "
      "\303\250\302\264\302\250\303\246\302\243\342\202\254\303\247\302\255\302"
      "\276\303\245\302\255\342\200\224\303\246\342\200\224\302\245\303\246\305"
      "\223\305\270 \303\250\302\257\342\200\236\303\244\302\273\302\267 "
      "\303\247\302\275\305\241\303\246\302\254\302\276100\303\245\342\200\246"
      "\306\222 \303\251\342\204\242\342\200\236\303\244\302\273\302\266 "
      "\303\250\302\257\302\264\303\246\313\234\305\275"};
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), doc_text1, base,
                                            base + 100, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), doc_text2,
                                            base + 1, base + 101, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), doc_text3,
                                            base + 2, base + 102, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), doc_text4,
                                            base + 3, base + 103, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), doc_text5,
                                            base + 4, base + 104, base + 69));
  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  SearchLogDebug("TextQueryUsingTokenize AddOrUpdateDocuments table id(%s)",
                 table.PrintToStr().c_str());
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }

  IndexConfig &index_config = index->Config();
  Tokenizer *tokenizer = index_config.GetTokenizer();
  wwsearch::Searcher searcher(&index_config);

  {
    std::string match_text1{"HT2018-12-020"};
    match_documentsid.clear();
    wwsearch::AndQuery query;
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenizer->BuildTerms(match_text1.c_str(), match_text1.size(), terms));

    SearchLogDebug("match_text1 tokenizer terms(%s)",
                   JoinContainerToString(terms, ";").c_str());
    for (auto &term : terms) {
      wwsearch::BooleanQuery *sub_query =
          new wwsearch::BooleanQuery(1, std::move(term));
      query.AddQuery(sub_query);
    }

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    SearchLogDebug("DoQuery match_documentsid(%s)",
                   JoinContainerToString(match_documentsid, ";").c_str());
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(3, match_documentsid.size());
  }

  {
    std::string match_text2{"HT-2018-12-020"};
    match_documentsid.clear();
    wwsearch::AndQuery query;
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenizer->BuildTerms(match_text2.c_str(), match_text2.size(), terms));

    SearchLogDebug("match_text2 tokenizer terms(%s)",
                   JoinContainerToString(terms, ";").c_str());
    for (auto &term : terms) {
      wwsearch::BooleanQuery *sub_query =
          new wwsearch::BooleanQuery(1, std::move(term));
      query.AddQuery(sub_query);
    }

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    SearchLogDebug("DoQuery match_documentsid(%s)",
                   JoinContainerToString(match_documentsid, ";").c_str());
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
  }
}

TEST_F(BoolQueryTest, Query_String_not_get_all) {
  auto base = GetNumeric(10000);
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "worlda", base,
                                            base + 100, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "xxxxx", base + 1,
                                            base + 101, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "worlda", base + 2,
                                            base + 102, base + 69));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  wwsearch::Searcher searcher(&index->Config());
  SearchTracer tracer;

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "worlda");
    auto status =
        searcher.DoQuery(table, query1, 0, 100, nullptr, nullptr,
                         match_documentsid, nullptr, 2000, 0, &tracer, nullptr);
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
    wwsearch::BooleanQuery query1(1, "worlda");
    auto status =
        searcher.DoQuery(table, query1, 0, 1, nullptr, nullptr,
                         match_documentsid, nullptr, 2000, 0, &tracer, nullptr);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }

  {
    uint32_t get_match_total_cnt = 0;
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "worlda");
    auto status = searcher.DoQuery(table, query1, 0, 1, nullptr, nullptr,
                                   match_documentsid, nullptr, 2000, 0, &tracer,
                                   &get_match_total_cnt);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    EXPECT_EQ(2, get_match_total_cnt);
    if (g_debug) {
      for (auto documentid : match_documentsid) {
        SearchLogDebug("match[%llu]\n", documentid);
      }
    }
  }
}
}  // namespace wwsearch
