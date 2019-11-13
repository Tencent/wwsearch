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
#include "checked.h"
#include "include/index_wrapper.h"
#include "include/tokenizer_mmseg.h"
#include "include/utf8_suffixbuilder.h"
#include "unchecked.h"
#include "unittest_util.h"

namespace wwsearch {

class TokenizeTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;

 public:
  TokenizeTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path =
        std::string("/tmp/unit_") + std::string("tokenize");
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

DefaultIndexWrapper *TokenizeTest::index = nullptr;
DocumentID TokenizeTest::document_id = 1;

TEST_F(TokenizeTest, AddOneDocument) {
  const char *chinese = "我是中国人，我爱中国。。。This is english。我爱中国";
  const char *dict = ".";

  SearchLogDebug("dict read check %s/uni.lib\n", dict);
  SearchLogDebug("str:%s\n", chinese);
  wwsearch::TokenizerMMSEG tokenize(dict, 2);
  auto document = TestUtil::NewDocument(GetDocumentID(), chinese, 1, 1, 2);
  tokenize.Do(document->New());

  auto field = document->New().FindField(1);
  for (auto term : field->Terms()) {
    SearchLogDebug("term:%s\n", term.c_str());
  }
}

TEST_F(TokenizeTest, AddOneDocumentAndMatch) {
  const std::string doc_text{
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
  const char *dict = ".";

  wwsearch::TokenizerMMSEG tokenize(dict, 2);
  auto document = TestUtil::NewDocument(GetDocumentID(), doc_text, 1, 1, 2);
  tokenize.Do(document->New());

  auto field = document->New().FindField(1);
  for (auto term : field->Terms()) {
    SearchLogDebug("term:%s\n", term.c_str());
  }

  {
    const std::string match_txt{"HT2018-12-020"};
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenize.BuildTerms(match_txt.c_str(), match_txt.size(), terms));
    SearchLogDebug("Search for match_txt (%s):\n", match_txt.c_str());
    for (const auto &term : terms) {
      SearchLogDebug("term:%s\n", term.c_str());
    }
  }

  {
    const std::string match_txt{"HT2018-12-020971"};
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenize.BuildTerms(match_txt.c_str(), match_txt.size(), terms));
    SearchLogDebug("Search for match_txt (%s):\n", match_txt.c_str());
    for (const auto &term : terms) {
      SearchLogDebug("term:%s\n", term.c_str());
    }
  }

  {
    const std::string match_txt{"12mm厚"};
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenize.BuildTerms(match_txt.c_str(), match_txt.size(), terms));
    SearchLogDebug("Search for match_txt (%s):\n", match_txt.c_str());
    for (const auto &term : terms) {
      SearchLogDebug("term:%s\n", term.c_str());
    }
  }

  {
    const std::string match_txt{"保存时间"};
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenize.BuildTerms(match_txt.c_str(), match_txt.size(), terms));
    SearchLogDebug("Search for match_txt (%s):\n", match_txt.c_str());
    for (const auto &term : terms) {
      SearchLogDebug("term:%s\n", term.c_str());
    }
  }

  {
    const std::string match_txt{"回库报告"};
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenize.BuildTerms(match_txt.c_str(), match_txt.size(), terms));
    SearchLogDebug("Search for match_txt (%s):\n", match_txt.c_str());
    for (const auto &term : terms) {
      SearchLogDebug("term:%s\n", term.c_str());
    }
  }

  {
    const std::string match_txt{"出库报告"};
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenize.BuildTerms(match_txt.c_str(), match_txt.size(), terms));
    SearchLogDebug("Search for match_txt (%s):\n", match_txt.c_str());
    for (const auto &term : terms) {
      SearchLogDebug("term:%s\n", term.c_str());
    }
  }

  {
    const std::string match_txt{"入库报告"};
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenize.BuildTerms(match_txt.c_str(), match_txt.size(), terms));
    SearchLogDebug("Search for match_txt (%s):\n", match_txt.c_str());
    for (const auto &term : terms) {
      SearchLogDebug("term:%s\n", term.c_str());
    }
  }

  {
    const std::string match_txt{"进库报告"};
    std::set<std::string> terms;
    EXPECT_TRUE(
        tokenize.BuildTerms(match_txt.c_str(), match_txt.size(), terms));
    SearchLogDebug("Search for match_txt (%s):\n", match_txt.c_str());
    for (const auto &term : terms) {
      SearchLogDebug("term:%s\n", term.c_str());
    }
  }
}

TEST_F(TokenizeTest, UTF8) {
  std::string chinese("我是中国人，我爱中国,....。。。。");
  bool error = false;
  char *it = (char *)chinese.c_str();
  char *begin = it;
  SearchLogDebug("chinese:%s\n", chinese.c_str());
  while (!error) {
    try {
      utf8::next(it, (char *)(begin + chinese.size()));
    } catch (...) {
      error = true;
      break;
    }
    SearchLogDebug(":%s\n", it);
  }
}

TEST(UTF8SuffixBuilderTest, UTF8SuffixBuilder) {
  std::string keywords{"helloworld"};
  UTF8SuffixBuilder builder(keywords.c_str(), keywords.size(), 5);
  SearchLogDebug("UTF8SuffixBuilder keywords(%s)output :\n", keywords.c_str());
  do {
    std::string term(builder.Term(), builder.TermSize());
    SearchLogDebug("%s,", term.c_str());
  } while (builder.Next());
  SearchLogDebug("\n");
}

}  // namespace wwsearch
