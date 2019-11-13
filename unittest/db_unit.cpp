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
class DbTest : public ::testing::Test {
 public:
  static DefaultIndexWrapper *index_;
  static uint64_t document_id_;
  static uint64_t numeric_value_;
  wwsearch::TableID table_;
  std::vector<DocumentUpdater *> documents_;
  std::list<DocumentID> match_documentsid_;

 public:
  DbTest() {
    table_.business_type = 1;
    table_.partition_set = 1;
  }

  static void SetUpTestCase() {
    index_ = new DefaultIndexWrapper();
    index_->DBParams().path = std::string("/tmp/unit_") + std::string("db");
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

DefaultIndexWrapper *DbTest::index_ = nullptr;
DocumentID DbTest::document_id_ = 1;
DocumentID DbTest::numeric_value_ = 1;

static std::string key{"hello"};
static std::string key1{"hello1"};
static std::string end_key{"hellp"};
static std::string value{"world"};
static std::string value1{"world1"};

TEST_F(DbTest, WriteAndReadCfKv) {
  {
    // write kv
    WriteBuffer *write_buffer = index_->vdb_->NewWriteBuffer(nullptr);
    write_buffer->Put(kMetaColumn, key, value);
    index_->vdb_->FlushBuffer(write_buffer);
    index_->vdb_->ReleaseWriteBuffer(write_buffer);
  }

  {
    // read kv
    VirtualDBSnapshot *snapshot = index_->vdb_->NewSnapshot();
    std::string read_value;
    SearchStatus s = index_->vdb_->Get(kMetaColumn, key, read_value, snapshot);
    EXPECT_TRUE(s.OK());
    EXPECT_TRUE(value.compare(read_value) == 0);
    index_->vdb_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(DbTest, WriteAndDeleteCfKv) {
  {
    // write kv
    WriteBuffer *write_buffer = index_->vdb_->NewWriteBuffer(nullptr);
    write_buffer->Put(kMetaColumn, key, value);
    index_->vdb_->FlushBuffer(write_buffer);
    index_->vdb_->ReleaseWriteBuffer(write_buffer);
  }

  {
    // write kv
    WriteBuffer *write_buffer = index_->vdb_->NewWriteBuffer(nullptr);
    write_buffer->Put(kMetaColumn, key1, value1);
    index_->vdb_->FlushBuffer(write_buffer);
    index_->vdb_->ReleaseWriteBuffer(write_buffer);
  }

  {
    // read kv
    VirtualDBSnapshot *snapshot = index_->vdb_->NewSnapshot();
    std::string read_value;
    SearchStatus s = index_->vdb_->Get(kMetaColumn, key, read_value, snapshot);
    EXPECT_TRUE(s.OK());
    EXPECT_TRUE(value.compare(read_value) == 0);
    index_->vdb_->ReleaseSnapshot(snapshot);
  }

  {
    // delete kv
    WriteBuffer *write_buffer = index_->vdb_->NewWriteBuffer(nullptr);
    write_buffer->Delete(kMetaColumn, key);
    index_->vdb_->FlushBuffer(write_buffer);
    index_->vdb_->ReleaseWriteBuffer(write_buffer);
  }

  {
    // read kv
    VirtualDBSnapshot *snapshot = index_->vdb_->NewSnapshot();
    std::string read_value;
    SearchStatus s = index_->vdb_->Get(kMetaColumn, key, read_value, snapshot);
    EXPECT_TRUE(s.DocumentNotExist());
    index_->vdb_->ReleaseSnapshot(snapshot);
  }

  {
    // read kv
    VirtualDBSnapshot *snapshot = index_->vdb_->NewSnapshot();
    std::string read_value;
    SearchStatus s = index_->vdb_->Get(kMetaColumn, key1, read_value, snapshot);
    EXPECT_TRUE(s.OK());
    EXPECT_TRUE(value1.compare(read_value) == 0);
    index_->vdb_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(DbTest, WriteAndDeleteRangeCfKv) {
  {
    // write kv
    WriteBuffer *write_buffer = index_->vdb_->NewWriteBuffer(nullptr);
    write_buffer->Put(kMetaColumn, key, value);
    index_->vdb_->FlushBuffer(write_buffer);
    index_->vdb_->ReleaseWriteBuffer(write_buffer);
  }

  {
    // write kv
    WriteBuffer *write_buffer = index_->vdb_->NewWriteBuffer(nullptr);
    write_buffer->Put(kMetaColumn, key1, value1);
    index_->vdb_->FlushBuffer(write_buffer);
    index_->vdb_->ReleaseWriteBuffer(write_buffer);
  }

  {
    // read kv
    VirtualDBSnapshot *snapshot = index_->vdb_->NewSnapshot();
    std::string read_value;
    SearchStatus s = index_->vdb_->Get(kMetaColumn, key, read_value, snapshot);
    EXPECT_TRUE(s.OK());
    EXPECT_TRUE(value.compare(read_value) == 0);
    index_->vdb_->ReleaseSnapshot(snapshot);
  }

  {
    // multi - read kv
    VirtualDBSnapshot *snapshot = index_->vdb_->NewSnapshot();
    std::vector<std::string> keys{key, key1};
    std::vector<std::string> read_values;
    std::vector<SearchStatus> ss;
    index_->vdb_->MultiGet(
        std::vector<StorageColumnType>{kMetaColumn, kMetaColumn}, keys,
        read_values, ss, snapshot);
    EXPECT_TRUE(read_values.size() == ss.size());
    for (auto &s : ss) {
      EXPECT_TRUE(s.OK());
    }
    EXPECT_TRUE(value.compare(read_values[0]) == 0);
    EXPECT_TRUE(value1.compare(read_values[1]) == 0);
    index_->vdb_->ReleaseSnapshot(snapshot);
  }

  {
    // delete kv
    WriteBuffer *write_buffer = index_->vdb_->NewWriteBuffer(nullptr);
    write_buffer->DeleteRange(kMetaColumn, key, end_key);
    index_->vdb_->FlushBuffer(write_buffer);
    index_->vdb_->ReleaseWriteBuffer(write_buffer);
  }

  {
    // read kv
    VirtualDBSnapshot *snapshot = index_->vdb_->NewSnapshot();
    std::string read_value;
    SearchStatus s = index_->vdb_->Get(kMetaColumn, key, read_value, snapshot);
    EXPECT_TRUE(s.DocumentNotExist());
    index_->vdb_->ReleaseSnapshot(snapshot);
  }

  {
    // read kv
    VirtualDBSnapshot *snapshot = index_->vdb_->NewSnapshot();
    std::string read_value;
    SearchStatus s = index_->vdb_->Get(kMetaColumn, key1, read_value, snapshot);
    EXPECT_TRUE(s.DocumentNotExist());
    index_->vdb_->ReleaseSnapshot(snapshot);
  }
}
}  // namespace wwsearch