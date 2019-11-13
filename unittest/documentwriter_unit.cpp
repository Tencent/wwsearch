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
#include "include/document.h"
#include "include/document_writer.h"
#include "include/index_wrapper.h"
#include "include/search_status.h"
#include "include/search_util.h"
#include "unittest_util.h"

extern bool g_debug;
extern bool g_use_rocksdb;
extern bool g_use_compression;

namespace wwsearch {
class DocumentWriterTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index_;
  static uint64_t document_id_;
  static uint64_t numeric_value_;
  wwsearch::TableID table_;
  std::vector<DocumentUpdater *> documents_;
  std::list<DocumentID> match_documentsid_;

 public:
  DocumentWriterTest() {
    table_.business_type = 1;
    table_.partition_set = 1;
  }

  static void SetUpTestCase() {
    index_ = new DefaultIndexWrapper();
    index_->DBParams().path =
        std::string("/tmp/unit_") + std::string("DocumentWriter");
    index_->Config().SetLogLevel(g_debug ? wwsearch::kSearchLogLevelDebug
                                         : wwsearch::kSearchLogLevelError);
    auto status = index_->Open();
    ASSERT_TRUE(status.GetCode() == 0);
  }

  static void TearDownTestCase() {
    if (index_ != nullptr) {
      index_->vdb_->DropDB();
      delete index_;
      index_ = nullptr;
    }
  }

  virtual void SetUp() override {
    table_.partition_set++;
    match_documentsid_.clear();
  }

  virtual void TearDown() override {
    for (auto du : documents_) {
      delete du;
    }
    documents_.clear();
    match_documentsid_.clear();
  }

  uint64_t GetDocumentID() { return document_id_++; }

  uint64_t GetNumeric(uint64_t alloc_len = 1000) {
    auto temp = numeric_value_;
    numeric_value_ += alloc_len;
    return temp;
  }

 private:
};

DefaultIndexWrapper *DocumentWriterTest::index_ = nullptr;
DocumentID DocumentWriterTest::document_id_ = 1;
DocumentID DocumentWriterTest::numeric_value_ = 1;

TEST_F(DocumentWriterTest, UpdateDocuments) {}

}  // namespace wwsearch