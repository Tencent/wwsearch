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
#include "include/codec_doclist.h"
#include "include/codec_doclist_impl.h"
#include "include/codec_impl.h"
#include "include/index_wrapper.h"
#include "include/search_util.h"
#include "unittest_util.h"

extern bool g_debug;
extern bool g_use_rocksdb;
extern bool g_use_compression;

namespace wwsearch {

class SearcherTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index;
  static uint64_t document_id;
  static uint64_t numeric_value;
  wwsearch::TableID table;
  std::vector<DocumentUpdater *> documents;
  std::list<DocumentID> match_documentsid;

 public:
  SearcherTest() {
    table.business_type = 1;
    table.partition_set = 1;
  }

  static void SetUpTestCase() {
    index = new DefaultIndexWrapper();
    index->DBParams().path =
        std::string("/tmp/unit_") + std::string("searcher");
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

DefaultIndexWrapper *SearcherTest::index = nullptr;
DocumentID SearcherTest::document_id = 1;
DocumentID SearcherTest::numeric_value = 1;

/*
TEST_F(SearcherTest, ScanBusinessType) {
  // 30 business type * 3 document
  VirtualDBSnapshot *snapshots[1];
  snapshots[0] = nullptr;
  this->table.business_type = 1;
  for (size_t base_set = 0; base_set < 30; base_set++) {
    {
      this->table.partition_set = base_set;
      auto base = GetNumeric(10000);
      documents.push_back(TestUtil::NewDocument(GetDocumentID(), "hello1", base,
                                                base + 100, base + 69));
      documents.push_back(TestUtil::NewDocument(
          GetDocumentID(), "girl1", base + 1, base + 101, base + 69));
      documents.push_back(TestUtil::NewDocument(
          GetDocumentID(), "hello1", base + 2, base + 102, base + 69));
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
    for (auto du : documents) {
      delete du;
    }
    documents.clear();
    if (base_set == 19) {
      snapshots[0] = index->vdb_->NewSnapshot();
    }
  }

  // we have 20  business type now
  for (auto item : snapshots) {
    ASSERT_TRUE(item != nullptr);
      wwsearch::Searcher searcher(&index->Config());
    uint8_t business_type = table.business_type;
    uint64_t start_partition_set = 0;
    std::vector<uint64_t> sets;
    VirtualDBSnapshot *snapshot = item;
    uint64_t base = 0;
    {
      sets.clear();
      start_partition_set = 0;
      do {
        auto status = searcher.ScanBusinessType(
            business_type, start_partition_set, 3, sets, snapshot);
        ASSERT_TRUE(status.OK());
      } while (start_partition_set != 0);
    }
    {
      sets.clear();
      start_partition_set = 0;
      auto status = searcher.ScanBusinessType(
          business_type, start_partition_set, 100, sets, snapshot);
      ASSERT_TRUE(status.OK());
      ASSERT_EQ(sets.size(), 20);
    }
    {
      sets.clear();
      start_partition_set = 0;
      auto status = searcher.ScanBusinessType(
          business_type, start_partition_set, 10, sets, snapshot);
      ASSERT_TRUE(status.OK());
      ASSERT_EQ(sets.size(), 10);
      for (auto set : sets) {
        EXPECT_TRUE(set >= 0 && set < 10);
      }
      EXPECT_TRUE(start_partition_set == 9);
      sets.clear();
      start_partition_set = 10;
      status = searcher.ScanBusinessType(business_type, start_partition_set, 10,
                                         sets, snapshot);
      ASSERT_TRUE(status.OK());
      ASSERT_EQ(sets.size(), 10);
      for (auto set : sets) {
        EXPECT_TRUE(set >= 10 && set < 20);
      }
    }

    if (snapshot != nullptr) {
      index->vdb_->ReleaseSnapshot(snapshot);
    }
  }
}
*/

TEST_F(SearcherTest, ScanTableData) {
  // 20 business type * 3 document
  this->table.business_type = 1;
  SearchTracer tracer;
  auto base = GetNumeric(10000);
  const uint32_t doc_num = 2019;
  for (int i = 0; i < doc_num; i++) {
    this->table.partition_set = 1000;
    documents.push_back(TestUtil::NewDocument(GetDocumentID(), "hello", base++,
                                              base++, base++));
  }
  bool ret = index->index_writer_->AddOrUpdateDocuments(table, documents,
                                                        nullptr, &tracer);
  EXPECT_TRUE(ret);

  /*
    kStoredFieldColumn = 0,    // store document
    kInvertedIndexColumn = 1,  // store invert doc list of match term
    kDocValueColumn = 2,       // store table doc value of every document
    kMetaColumn = 3,           // store user'id mapping currently
    kDictionaryColumn = 4,     // store nothing
    */
  {
    wwsearch::Searcher searcher(&index->Config());
    uint8_t business_type = table.business_type;
    uint64_t start_partition_set = 0;
    std::vector<uint64_t> sets;
    VirtualDBSnapshot *snapshot = index->vdb_->NewSnapshot();
    uint64_t base = 0;
    start_partition_set = 0;
    wwsearch::StorageColumnType columns[] = {
        kStoredFieldColumn,
        kInvertedIndexColumn,  // store invert doc list of match term
        kDocValueColumn,       // store table doc value of every document
        kMetaColumn,           // store user'id mapping currently
        kDictionaryColumn      // store nothing
    };
    int columns_expect_keys_delta[] = {1, 3, 1, 0, 0};
    int columns_expect_keys_constant[] = {0, 1, 0, 0, 0};

    for (size_t i = 0; i < sizeof(columns) / sizeof(StorageColumnType); i++) {
      std::string write_batch;
      std::string start_key;
      uint64_t count = 0;
      int total_key_count = 0;
      do {
        wwsearch::SearchStatus status;
        status = searcher.ScanTableData(table, columns[i], start_key, 10,
                                        write_batch, snapshot);
        ASSERT_TRUE(status.OK());
        if (!start_key.empty()) {
          count++;
          EXPECT_TRUE(write_batch.size() != 0);
        }
        WriteBuffer *write_buffer = index->vdb_->NewWriteBuffer(&write_batch);
        total_key_count += write_buffer->KvCount();
        index->vdb_->ReleaseWriteBuffer(write_buffer);
        // rocksdb::WriteBatch batch(write_batch);
        // total_key_count += batch.Count();
        write_batch.clear();
      } while (!start_key.empty());

      int expected_count = doc_num * columns_expect_keys_delta[i] +
                           columns_expect_keys_constant[i];
      SearchLogDebug("expect:%d,real:%d,count:%d\n", expected_count,
                     total_key_count, count);
      EXPECT_EQ(total_key_count, expected_count);

      if (i < 3) {
        EXPECT_TRUE(count > 0);
      }
    }

    index->vdb_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(SearcherTest, DocListOrderWriterCodecImplDebug) {
  std::unique_ptr<wwsearch::Codec> codec(new wwsearch::CodecImpl);
  std::string data;
  wwsearch::DocListWriterCodec *doc_list_order_writer_codec =
      codec->NewOrderDocListWriterCodec();
  // must keep decrease order
  doc_list_order_writer_codec->AddDocID(456, 2);
  doc_list_order_writer_codec->AddDocID(123, 1);
  // doc_list_order_writer_codec->AddDocID(456, 2);
  SearchLogDebug("DocListWriterCodec debug : %s\n",
                 doc_list_order_writer_codec->DebugString().c_str());
  codec->ReleaseOrderDocListWriterCodec(doc_list_order_writer_codec);
}

TEST_F(SearcherTest, DocListReaderCodecImplTest1) {
  std::unique_ptr<wwsearch::Codec> codec(new wwsearch::CodecImpl);
  std::string data;
  {
    wwsearch::DocListWriterCodec *doc_list_order_writer_codec =
        codec->NewOrderDocListWriterCodec();
    doc_list_order_writer_codec->AddDocID(10, 1);
    doc_list_order_writer_codec->AddDocID(8, 0);
    doc_list_order_writer_codec->AddDocID(5, 0);
    doc_list_order_writer_codec->AddDocID(3, 0);
    SearchLogDebug("DocListWriterCodec debug : %s\n",
                   doc_list_order_writer_codec->DebugString().c_str());
    doc_list_order_writer_codec->SerializeToBytes(data, 0);
    codec->ReleaseOrderDocListWriterCodec(doc_list_order_writer_codec);
  }
  {
    // small all
    DocumentID target = 2;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(wwsearch::DocIdSetIterator::NO_MORE_DOCS, reader.Advance(target));
  }
  {
    // bigger all
    DocumentID target = 13;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(10, reader.Advance(target));
  }
  {
    // not include search
    DocumentID target = 6;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(5, reader.Advance(target));
  }
  {
    // not include search
    DocumentID target = 4;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(3, reader.Advance(target));
  }
  {
    // include search
    DocumentID target = 5;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(5, reader.Advance(target));
  }
  {
    // include search
    DocumentID target = 8;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(8, reader.Advance(target));
  }
  {
    DocumentID target = 3;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(3, reader.Advance(target));
  }
  {
    DocumentID target = 10;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(10, reader.Advance(target));
  }
}

TEST_F(SearcherTest, DocListReaderCodecImplTest2) {
  std::unique_ptr<wwsearch::Codec> codec(new wwsearch::CodecImpl);
  std::string data;
  {
    wwsearch::DocListWriterCodec *doc_list_order_writer_codec =
        codec->NewOrderDocListWriterCodec();
    doc_list_order_writer_codec->AddDocID(18, 1);
    doc_list_order_writer_codec->AddDocID(13, 0);
    doc_list_order_writer_codec->AddDocID(8, 0);
    doc_list_order_writer_codec->AddDocID(6, 0);
    doc_list_order_writer_codec->AddDocID(3, 0);
    SearchLogDebug("DocListWriterCodec debug : %s\n",
                   doc_list_order_writer_codec->DebugString().c_str());
    doc_list_order_writer_codec->SerializeToBytes(data, 0);
    codec->ReleaseOrderDocListWriterCodec(doc_list_order_writer_codec);
  }
  {
    // small all
    DocumentID target = 2;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(wwsearch::DocIdSetIterator::NO_MORE_DOCS, reader.Advance(target));
  }
  {
    // bigger all
    DocumentID target = 23;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(18, reader.Advance(target));
  }
  {
    // not include search
    DocumentID target = 7;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(6, reader.Advance(target));
  }
  {
    // not include search
    DocumentID target = 9;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(8, reader.Advance(target));
  }
  {
    // include search
    DocumentID target = 6;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(6, reader.Advance(target));
  }
  {
    // include search
    DocumentID target = 8;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(8, reader.Advance(target));
  }
  {
    DocumentID target = 18;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(18, reader.Advance(target));
  }
  {
    DocumentID target = 3;
    wwsearch::DocListReaderCodecImpl reader(data.c_str(), data.size());
    EXPECT_EQ(3, reader.Advance(target));
  }
}
}  // namespace wwsearch
