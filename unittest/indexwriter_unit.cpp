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

class IndexWriterTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  static uint64_t numeric_value;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;
  std::list<DocumentID> match_documentsid;

 public:
  IndexWriterTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path =
        std::string("/tmp/unit_") + std::string("indexwriter");
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

  void Clear() {
    for (auto du : documents) {
      delete du;
    }
    documents.clear();
  }

  uint64_t GetDocumentID() { return document_id++; }

  uint64_t GetNumeric(uint64_t alloc_len = 1000) {
    auto temp = numeric_value;
    numeric_value += alloc_len;
    return temp;
  }

 private:
};

DefaultIndexWrapper *IndexWriterTest::index = nullptr;
DocumentID IndexWriterTest::document_id = 1;
DocumentID IndexWriterTest::numeric_value = 1;

TEST_F(IndexWriterTest, AddDocumentsEmtpy) {
  this->table.business_type = 1;
  SearchTracer tracer;
  // std::string field_str{"hello1，我是中文字符PAXOSSEARCHpaxossearch1929"};
  std::string field_str{"hello123456789"};
  uint64_t doc_id = GetDocumentID();
  this->table.partition_set = 1000;
  auto base = GetNumeric(10000);
  documents.push_back(
      TestUtil::NewDocument(doc_id, field_str, base, base + 100, base + 69));
  bool ret =
      index->index_writer_->AddDocuments(table, documents, nullptr, &tracer);
  EXPECT_TRUE(ret);

  {  // store field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    SearchLogDebug(
        "table.business_type = %d, table.partition_set = %d, doc_id = %d",
        this->table.business_type, this->table.partition_set, doc_id);
    std::string value;
    index->vdb_->Get(kStoredFieldColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 1) {
        SearchLogDebug("old_field_str[%d, %s] get_field_str[%d, %s]",
                       field_str.size(), field_str.c_str(),
                       field->StringValue().size(),
                       field->StringValue().c_str());
        EXPECT_TRUE(field->StringValue().compare(field_str) == 0);
      }
    }
  }

  {  // docvalue field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kDocValueColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 1) {
        SearchLogDebug("old_field_str[%d, %s] get_field_str[%d, %s]",
                       field_str.size(), field_str.c_str(),
                       field->StringValue().size(),
                       field->StringValue().c_str());
        EXPECT_TRUE(field->StringValue().compare(field_str) == 0);
      }
    }
  }
}

TEST_F(IndexWriterTest, AddDocumentsDuplicate) {
  this->table.business_type = 1;
  SearchTracer tracer;
  std::string field_str{"hello1，我是中文字符PAXOSSEARCHpaxossearch1929"};
  uint64_t doc_id = GetDocumentID();
  this->table.partition_set = 1000;
  auto base = GetNumeric(10000);
  DocumentUpdater *doc_updater =
      TestUtil::NewDocument(doc_id, field_str, base, base + 100, base + 69);
  documents.push_back(doc_updater);
  bool ret =
      index->index_writer_->AddDocuments(table, documents, nullptr, &tracer);
  EXPECT_TRUE(ret);

  ret = index->index_writer_->AddDocuments(table, documents, nullptr, &tracer);
  EXPECT_FALSE(ret);
  Clear();
}

TEST_F(IndexWriterTest, UpdateDocumentsAndCheck) {
  this->table.business_type = 1;
  SearchTracer tracer;
  std::string field_str{"hello1，我是中文字符PAXOSSEARCHpaxossearch1929"};
  uint64_t doc_id = GetDocumentID();
  this->table.partition_set = 1000;
  auto base = GetNumeric(10000);
  DocumentUpdater *doc_updater =
      TestUtil::NewDocument(doc_id, field_str, base, base + 100, base + 69);
  documents.push_back(doc_updater);
  bool ret =
      index->index_writer_->AddDocuments(table, documents, nullptr, &tracer);
  EXPECT_TRUE(ret);

  std::string field_str2{"world!!!"};
  Clear();
  this->table.partition_set = 1000;
  base = GetNumeric(10000);
  doc_updater =
      TestUtil::NewDocument(doc_id, field_str2, base, base + 100, base + 69);
  documents.push_back(doc_updater);

  ret =
      index->index_writer_->UpdateDocuments(table, documents, nullptr, &tracer);
  EXPECT_TRUE(ret);

  {  // store field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kStoredFieldColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 1) {
        EXPECT_TRUE(field->StringValue().compare(field_str2) == 0);
      }
    }
  }

  {  // docvalue field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kDocValueColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 1) {
        EXPECT_TRUE(field->StringValue().compare(field_str2) == 0);
      }
    }
  }
  Clear();
}

TEST_F(IndexWriterTest, AddOrUpdateDocumentsAndCheck) {
  this->table.business_type = 1;
  SearchTracer tracer;
  std::string field_str{"hello1，我是中文字符PAXOSSEARCHpaxossearch1929"};
  uint64_t doc_id = GetDocumentID();
  this->table.partition_set = 1000;
  auto base = GetNumeric(10000);
  DocumentUpdater *doc_updater =
      TestUtil::NewDocument(doc_id, field_str, base, base + 100, base + 69);
  documents.push_back(doc_updater);
  bool ret =
      index->index_writer_->AddDocuments(table, documents, nullptr, &tracer);
  EXPECT_TRUE(ret);

  std::string field_str2{"world!!!"};
  Clear();
  this->table.partition_set = 1000;
  base = GetNumeric(10000);
  doc_updater =
      TestUtil::NewDocument(doc_id, field_str2, base, base + 100, base + 69);
  documents.push_back(doc_updater);

  ret = index->index_writer_->AddOrUpdateDocuments(table, documents, nullptr,
                                                   &tracer);
  EXPECT_TRUE(ret);

  {  // store field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kStoredFieldColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 1) {
        EXPECT_TRUE(field->StringValue().compare(field_str2) == 0);
      }
    }
  }

  {  // docvalue field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kDocValueColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 1) {
        EXPECT_TRUE(field->StringValue().compare(field_str2) == 0);
      }
    }
  }
  Clear();
}

TEST_F(IndexWriterTest, UpdateFieldAndCheck) {
  this->table.business_type = 1;
  SearchTracer tracer;
  std::string field_str{"hello1，我是中文字符PAXOSSEARCHpaxossearch1929"};
  uint64_t doc_id = GetDocumentID();
  this->table.partition_set = 1000;
  auto base = GetNumeric(10000);
  DocumentUpdater *doc_updater =
      TestUtil::NewDocument(doc_id, field_str, base, base + 100, base + 69);
  documents.push_back(doc_updater);
  bool ret =
      index->index_writer_->AddDocuments(table, documents, nullptr, &tracer);
  EXPECT_TRUE(ret);

  std::string field_str2{"world!!!"};
  Clear();
  this->table.partition_set = 1000;
  base = GetNumeric(10000);
  doc_updater =
      TestUtil::NewDocument(doc_id, field_str2, base, base + 100, base + 69, 5);
  documents.push_back(doc_updater);

  ret = index->index_writer_->AddOrUpdateDocuments(table, documents, nullptr,
                                                   &tracer);
  EXPECT_TRUE(ret);

  {  // store field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kStoredFieldColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 1) {
        EXPECT_TRUE(field->StringValue().compare(field_str) == 0);
      }
    }
  }

  {  // docvalue field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kDocValueColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 1) {
        EXPECT_TRUE(field->StringValue().compare(field_str) == 0);
      }
    }
  }

  {  // store field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kStoredFieldColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 5) {
        EXPECT_TRUE(field->StringValue().compare(field_str2) == 0);
      }
    }
  }

  {  // docvalue field
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    index->vdb_->Get(kDocValueColumn, key, value, nullptr);
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
    for (IndexField *field : document.Fields()) {
      if (field->ID() == 5) {
        EXPECT_TRUE(field->StringValue().compare(field_str2) == 0);
      }
    }
  }
  Clear();
}

TEST_F(IndexWriterTest, DeleteTableData) {
  // 20 business type * 3 document
  this->table.business_type = 1;
  SearchTracer tracer;
  {
    this->table.partition_set = 1000;
    auto base = GetNumeric(10000);
    documents.push_back(TestUtil::NewDocument(
        GetDocumentID(), "hello1，我是中文字符PAXOSSEARCHpaxossearch1929", base,
        base + 100, base + 69));
  }
  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, &tracer);
  EXPECT_TRUE(ret);

  for (size_t column = 0; column < kMaxColumn; column++) {
    // delete
    std::string start_key;
    do {
      auto status = index->index_writer_->DeleteTableData(
          table, (StorageColumnType)column, start_key, 10);
      ASSERT_TRUE(status.OK());
    } while (!start_key.empty());

    SearchLogDebug("");

    // db is empty now.
    VirtualDBReadOption options;
    auto iterator =
        index->vdb_->NewIterator((StorageColumnType)column, &options);
    iterator->SeekToFirst();
    EXPECT_FALSE(iterator->Valid());
    delete iterator;
  }
  Clear();
}

TEST_F(IndexWriterTest, AcquireSequence) {
  // 20 business type * 3 document
  this->table.business_type = 1;
  SearchTracer tracer;
  this->table.partition_set = 1000;
  bool ret;
  uint64_t current_max;
  std::vector<wwsearch::DocumentWriter::AllocSequence> sequence;
  // GetCurrent
  ASSERT_TRUE(index->index_writer_->AcquireCurrentSequence(table, current_max,
                                                           sequence, &tracer));
  ASSERT_TRUE(0 == current_max);

  wwsearch::DocumentWriter::AllocSequence seq1, seq2;
  seq1.user_id.assign("sdfdksfksk");
  seq2.user_id.assign("sdfiweiii");
  sequence.push_back(seq1);
  sequence.push_back(seq2);
  // Add Mapping twice
  // Return same info
  for (int i = 0; i < 2; i++) {
    ASSERT_TRUE(index->index_writer_->AcquireNewSequence(table, sequence,
                                                         nullptr, &tracer));
    for (int j = 0; j < 2; j++) {
      ASSERT_TRUE(sequence[j].status.OK());
      ASSERT_TRUE(sequence[j].sequence == j + 1);
    }
  }
  // GetCurrent Again
  ASSERT_TRUE(index->index_writer_->AcquireCurrentSequence(table, current_max,
                                                           sequence, &tracer));
  ASSERT_TRUE(2 == current_max);
}

TEST_F(IndexWriterTest, DropTable) {
  SearchTracer tracer;
  this->table.business_type = 10;
  this->table.partition_set = 999;

  uint64_t doc_id = GetDocumentID();
  for (int i = 0; i < 100; ++i) {
    std::string field_str = std::string{"hello123456789"} + std::to_string(i);
    auto base = GetNumeric(10000);
    documents.clear();
    documents.push_back(TestUtil::NewDocument(doc_id + i, field_str, base,
                                              base + 100, base + 69));
    bool ret =
        index->index_writer_->AddDocuments(table, documents, nullptr, &tracer);
    EXPECT_TRUE(ret);
  }

  {
    std::string field_str = std::string{"hello123456789"};
    auto base = GetNumeric(10000);
    wwsearch::DocumentUpdater *doc_updater = TestUtil::NewDocument(
        doc_id + 1000, field_str, base, base + 100, base + 69);
    InitStringField(doc_updater->New().AddField(), UINT8_MAX,
                    std::string{"123"});
    std::vector<wwsearch::DocumentUpdater *> doc_updater_list{doc_updater};
    bool ret = index->index_writer_->AddDocuments(table, doc_updater_list,
                                                  nullptr, &tracer);
    EXPECT_TRUE(ret);
  }

  {
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    SearchStatus s = index->vdb_->Get(kStoredFieldColumn, key, value, nullptr);
    EXPECT_TRUE(s.OK());
    Document document;
    EXPECT_TRUE(document.DeSerializeFromByte(value.c_str(), value.size()));
    EXPECT_TRUE(document.ID() == doc_id);
  }

  {
    // read inverted index
    std::string key;
    index->codec_->EncodeInvertedKey(table, UINT8_MAX, std::string{"123"}, key);
    std::string value;
    SearchStatus s =
        index->vdb_->Get(kInvertedIndexColumn, key, value, nullptr);
    EXPECT_TRUE(s.OK());
  }

  bool ret = index->index_writer_->DropTable(this->table, nullptr, nullptr);
  EXPECT_TRUE(ret == true);

  {
    std::string key;
    index->codec_->EncodeStoredFieldKey(table, doc_id, key);
    std::string value;
    SearchStatus s = index->vdb_->Get(kStoredFieldColumn, key, value, nullptr);
    EXPECT_TRUE(s.DocumentNotExist());
  }

  {
    // read inverted index
    std::string key;
    index->codec_->EncodeInvertedKey(table, UINT8_MAX, std::string{"123"}, key);
    std::string value;
    SearchStatus s =
        index->vdb_->Get(kInvertedIndexColumn, key, value, nullptr);
    EXPECT_TRUE(s.DocumentNotExist());
  }
}

}  // namespace wwsearch
