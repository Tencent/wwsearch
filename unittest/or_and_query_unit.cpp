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

class OrAndQueryTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  static uint64_t numeric_value;
  static wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;
  std::list<DocumentID> match_documentsid;

 public:
  OrAndQueryTest() {}

  virtual ~OrAndQueryTest() {}

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
    table.business_type++;
    table.partition_set++;
    documents.clear();
    match_documentsid.clear();
  }

  void VariableChange() {
    table.business_type++;
    table.partition_set++;
    documents.clear();
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

DefaultIndexWrapper *OrAndQueryTest::index = nullptr;
DocumentID OrAndQueryTest::document_id = 1;
DocumentID OrAndQueryTest::numeric_value = 1;
wwsearch::TableID OrAndQueryTest::table{1, 100};

TEST_F(OrAndQueryTest, Query_String) {
  VariableChange();

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
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query2(1, "girla");
    auto status = searcher.DoQuery(table, query2, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query3(1, "nothing1");
    auto status = searcher.DoQuery(table, query3, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }
}

TEST_F(OrAndQueryTest, And_Query_String) {
  VariableChange();
  auto base = GetNumeric(10000);
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloc worldc",
                                            base, base + 100, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "girlc worldc",
                                            base + 1, base + 101, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "helloc worldc",
                                            base + 2, base + 102, base + 69));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "helloc");
    wwsearch::BooleanQuery query2(1, "worldc");
    wwsearch::AndQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "girlc");
    wwsearch::BooleanQuery query2(1, "worldc");
    wwsearch::AndQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "girlc");
    wwsearch::BooleanQuery query2(1, "noexist3");
    wwsearch::AndQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }
}

TEST_F(OrAndQueryTest, Or_Query_String) {
  VariableChange();
  auto base = GetNumeric(10000);
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "hellod worldd",
                                            base, base + 100, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "hellod girld",
                                            base + 1, base + 101, base + 69));
  documents.push_back(TestUtil::NewDocument(GetDocumentID(), "hellod worldd",
                                            base + 2, base + 102, base + 69));
  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "hellod");
    wwsearch::BooleanQuery query2(1, "worldd");
    wwsearch::OrQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(3, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "girld");
    wwsearch::BooleanQuery query2(1, "worldd");
    wwsearch::OrQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(3, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "girld");
    wwsearch::BooleanQuery query2(1, "noexist4");
    wwsearch::OrQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }
}

TEST_F(OrAndQueryTest, And_Query_Multi_String) {
  VariableChange();
  auto base = GetNumeric(10000);

  std::vector<TestUtil::FieldIdStrPair> field_id_str_list1{
      {1, "a"}, {2, "b"}, {3, "c"}};
  std::vector<TestUtil::FieldIdStrPair> field_id_str_list2{
      {4, "d"}, {5, "e"}, {6, "f"}};
  std::vector<TestUtil::FieldIdStrPair> field_id_str_list3{
      {7, "g"}, {8, "h"}, {9, "i"}};
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list1));
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list2));
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list3));

  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "a");
    wwsearch::BooleanQuery query2(2, "c");
    wwsearch::AndQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "a");
    wwsearch::BooleanQuery query2(3, "c");
    wwsearch::AndQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }
}

TEST_F(OrAndQueryTest, Or_Query_Multi_String) {
  VariableChange();
  auto base = GetNumeric(10000);
  std::vector<TestUtil::FieldIdStrPair> field_id_str_list1{
      {1, "a"}, {2, "b"}, {3, "c"}};
  std::vector<TestUtil::FieldIdStrPair> field_id_str_list2{
      {4, "d"}, {5, "e"}, {6, "f"}};
  std::vector<TestUtil::FieldIdStrPair> field_id_str_list3{
      {7, "g"}, {8, "h"}, {9, "i"}};
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list1));
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list2));
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list3));
  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "a");
    wwsearch::BooleanQuery query2(2, "b");
    wwsearch::OrQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "a");
    wwsearch::BooleanQuery query2(4, "d");
    wwsearch::OrQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "a");
    wwsearch::BooleanQuery query2(2, "c");
    wwsearch::OrQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(3, "a");
    wwsearch::BooleanQuery query2(4, "c");
    wwsearch::OrQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(0, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();
    wwsearch::BooleanQuery query1(1, "a");
    wwsearch::BooleanQuery query2(4, "d");
    wwsearch::BooleanQuery query3(8, "h");
    wwsearch::OrQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);
    query.AddQuery(&query3);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(3, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }
}

TEST_F(OrAndQueryTest, UsePostScorerForPrefixTextQuery) {
  std::string keyword{"hello world"};
  std::string keyword2{"fucking work"};
  VariableChange();
  auto base = GetNumeric(10000);
  std::vector<TestUtil::FieldIdStrPair> field_id_str_list1{
      {1, "a"}, {2, "b"}, {3, "c"}};
  std::vector<TestUtil::FieldIdStrPair> field_id_str_list2{
      {4, "d"}, {5, "e"}, {6, keyword2}};
  std::vector<TestUtil::FieldIdStrPair> field_id_str_list3{
      {7, "g"}, {8, "h"}, {9, "i"}, {10, keyword}};
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list1));
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list2));
  documents.push_back(
      TestUtil::NewStringFieldDocument(GetDocumentID(), field_id_str_list3));
  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, nullptr);
  EXPECT_TRUE(ret);
  for (auto du : documents) {
    EXPECT_EQ(0, du->Status().GetCode());
  }

  wwsearch::Searcher searcher(&index->Config());

  {
    match_documentsid.clear();

    std::vector<std::shared_ptr<wwsearch::ScoreStrategy>> score_strategy_list{
        wwsearch::ScoreStrategyFactory::NewScoreStrategy(keyword, 2, 10)};

    wwsearch::PrefixQuery query1(10, "hel");
    wwsearch::PrefixQuery query2(10, "wor");
    wwsearch::AndQuery query;
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    auto status = searcher.DoQuery(table, query, 0, 100, nullptr, nullptr,
                                   match_documentsid, &score_strategy_list, 20);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();

    std::vector<std::shared_ptr<wwsearch::ScoreStrategy>> score_strategy_list{
        wwsearch::ScoreStrategyFactory::NewScoreStrategy(keyword, 2, 10)};

    wwsearch::PrefixQuery query1(10, "hel");
    wwsearch::PrefixQuery query2(10, "wor");
    wwsearch::AndQuery sub_query1;
    sub_query1.AddQuery(&query1);
    sub_query1.AddQuery(&query2);

    wwsearch::PrefixQuery query3(6, "fuck");
    wwsearch::PrefixQuery query4(6, "wo");
    wwsearch::AndQuery sub_query2;
    sub_query2.AddQuery(&query3);
    sub_query2.AddQuery(&query4);

    wwsearch::OrQuery query;
    query.AddQuery(&sub_query1);
    query.AddQuery(&sub_query2);

    auto status = searcher.DoQuery(table, query, 0, 2, nullptr, nullptr,
                                   match_documentsid, &score_strategy_list, 20);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();

    std::vector<std::shared_ptr<wwsearch::ScoreStrategy>> score_strategy_list{
        wwsearch::ScoreStrategyFactory::NewScoreStrategy(keyword, 2, 10)};

    wwsearch::PrefixQuery query1(10, "hel");
    wwsearch::PrefixQuery query2(10, "wor");
    wwsearch::AndQuery sub_query1;
    sub_query1.AddQuery(&query1);
    sub_query1.AddQuery(&query2);

    wwsearch::PrefixQuery query3(6, "fuck");
    wwsearch::PrefixQuery query4(6, "wo");
    wwsearch::AndQuery sub_query2;
    sub_query2.AddQuery(&query3);
    sub_query2.AddQuery(&query4);

    wwsearch::OrQuery query;
    query.AddQuery(&sub_query1);
    query.AddQuery(&sub_query2);

    auto status = searcher.DoQuery(table, query, 0, 2, nullptr, nullptr,
                                   match_documentsid, &score_strategy_list, 2);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(2, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }

  {
    match_documentsid.clear();

    std::vector<std::shared_ptr<wwsearch::ScoreStrategy>> score_strategy_list{
        wwsearch::ScoreStrategyFactory::NewScoreStrategy(keyword, 2, 10)};

    wwsearch::PrefixQuery query1(10, "hel");
    wwsearch::PrefixQuery query2(10, "wor");
    wwsearch::AndQuery sub_query1;
    sub_query1.AddQuery(&query1);
    sub_query1.AddQuery(&query2);

    wwsearch::PrefixQuery query3(6, "fuck");
    wwsearch::PrefixQuery query4(6, "wo");
    wwsearch::AndQuery sub_query2;
    sub_query2.AddQuery(&query3);
    sub_query2.AddQuery(&query4);

    wwsearch::OrQuery query;
    query.AddQuery(&sub_query1);
    query.AddQuery(&sub_query2);

    auto status = searcher.DoQuery(table, query, 0, 1, nullptr, nullptr,
                                   match_documentsid, &score_strategy_list, 2);
    EXPECT_EQ(0, status.GetCode());
    EXPECT_EQ(1, match_documentsid.size());
    SearchLogDebug(
        "table business_id{%u}, partition_set{%llu}, match_documentsid : {%s}",
        table.business_type, table.partition_set,
        JoinContainerToString(match_documentsid, ", ").c_str());
  }
}
}  // namespace wwsearch
