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

#pragma once

#include "codec.h"
#include "codec_doclist_impl.h"
#include "header.h"
#include "virtual_db.h"

#include "db/db_impl.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/db_ttl.h"
#include "virtual_db_rocks_write_queue.h"

namespace wwsearch {

namespace merge {
class OptimizeMerger;
}

// Simple merger for test
class ConcatMergeOperator : public rocksdb::MergeOperator {
 private:
 public:
  // Constructor
  ConcatMergeOperator() {}
  virtual ~ConcatMergeOperator() {}

  // merge all value of one key in db.
  virtual bool FullMergeV2(
      const rocksdb::MergeOperator::MergeOperationInput& merge_in,
      rocksdb::MergeOperator::MergeOperationOutput* merge_out) const override {
    if (merge_in.existing_value) {
      merge_out->new_value.assign(merge_in.existing_value->data(),
                                  merge_in.existing_value->size());
    }
    for (auto item : merge_in.operand_list) {
      merge_out->new_value.append(item.data(), item.size());
    }
    return true;
  }

  // merge some value of one key  in db.
  virtual bool PartialMergeMulti(const rocksdb::Slice& key,
                                 const std::deque<rocksdb::Slice>& operand_list,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const override {
    for (auto item : operand_list) {
      new_value->append(item.data(), item.size());
      return true;
    }
  }

  // Return name of the merger.
  virtual const char* Name() const override { return "ConcatMergeOperator"; }

 private:
};

// Another optimize merger.
namespace merge {
struct DocList {
  DocumentID doc_id_;
  DocumentState doc_state_;
} __attribute__((packed));
typedef struct DocList DocList;
}  // namespace merge

// This merger will merge doclist into one doclist.
class DocListMergeOperator : public rocksdb::MergeOperator {
 private:
  Codec* codec_;
  merge::OptimizeMerger* merger_;

 public:
  // Constructor
  DocListMergeOperator(Codec* codec, merge::OptimizeMerger* merger)
      : codec_(codec), merger_(merger) {}

  virtual ~DocListMergeOperator();

  // New global single instance
  static DocListMergeOperator* NewInstance(VDBParams* params);

  // merge all doclist of inverted index's value with order.
  virtual bool FullMergeV2(
      const rocksdb::MergeOperator::MergeOperationInput& merge_in,
      rocksdb::MergeOperator::MergeOperationOutput* merge_out) const override;

  // merge some doclist of inverted index's value with order.
  virtual bool PartialMergeMulti(const rocksdb::Slice& key,
                                 const std::deque<rocksdb::Slice>& operand_list,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const override;

  // deprecated api
  virtual bool FullMerge(const rocksdb::Slice& key,
                         const rocksdb::Slice* existing_value,
                         const std::deque<std::string>& operand_list,
                         std::string* new_value,
                         rocksdb::Logger* logger) const override;

  // deprecated api
  virtual bool PartialMerge(const rocksdb::Slice& key,
                            const rocksdb::Slice& left_operand,
                            const rocksdb::Slice& right_operand,
                            std::string* new_value,
                            rocksdb::Logger* logger) const override;

  // TODO:
  // support multi partial merge
  // support full merge v2
  // support shouldmerge()

  // Do not change the merge name.
  virtual const char* Name() const override { return "DocListMergeOperator"; }

 private:
  // Inner api do the real merge job
  bool DocListMerge(std::vector<DocListReaderCodec*>& items,
                    std::string* new_value, rocksdb::Logger* logger,
                    bool purge_deleted = false) const;

  // Decode packed doclist into fixed size doclist.
  std::pair<merge::DocList*, size_t> DecodeDocList(
      std::vector<std::string>& alloc_buffer, const char* data,
      size_t data_len) const;

  // Inner api do the real merge job
  bool DoMerge(std::string* new_value,
               std::vector<std::pair<merge::DocList*, size_t>>& doc_lists,
               bool full_merge) const;
};

// RocksDB snapshot wrapper
class VirtualDBRocksSnapshot : public VirtualDBSnapshot {
 private:
  const rocksdb::Snapshot* snapshot_;

 public:
  VirtualDBRocksSnapshot(const rocksdb::Snapshot* snapshot)
      : snapshot_(snapshot) {}
  virtual ~VirtualDBRocksSnapshot() {}
  // Get current snapshot.
  inline const rocksdb::Snapshot* GetSnapshot() { return this->snapshot_; }

 private:
};

// RocksDB read option wrapper
class VirtualDBRocksReadOption : public VirtualDBReadOption {
 public:
  virtual ~VirtualDBRocksReadOption() {}

 private:
};

// RocksDB write option wrapper
class VirtualDBRocksWriteOption : public VirtualDBWriteOption {
 public:
  VirtualDBRocksWriteOption() : options_(rocksdb::WriteOptions()) {}
  virtual ~VirtualDBRocksWriteOption() {}
  rocksdb::WriteOptions& GetWriteOptions() { return options_; }

 private:
  rocksdb::WriteOptions options_;
};

// RocksDB wrapper
class VirtualDBRocksImpl : public VirtualDB {
 private:
  // outer reference
  VDBParams* params_;
  // new reference.
  rocksdb::DB* db_;
  DocListMergeOperator* merger_;
  // rocksdb::DBWithTTL* db_;
  rocksdb::Options options_;
  std::vector<rocksdb::ColumnFamilyHandle*> column_famil_handles_;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families_;
  std::shared_ptr<DbRocksWriteQueue> write_queue_;

 public:
  explicit VirtualDBRocksImpl(
      VDBParams* params, const std::shared_ptr<DbRocksWriteQueue>& write_queue);

  ~VirtualDBRocksImpl();

  virtual bool Open() override;

  virtual rocksdb::DB* GetDb() { return db_; }

  virtual std::vector<rocksdb::ColumnFamilyHandle*>& ColumnFamilyHandle();

  virtual VirtualDBSnapshot* NewSnapshot();

  virtual void ReleaseSnapshot(VirtualDBSnapshot*);

  // Flush to db.
  virtual SearchStatus FlushBuffer(WriteBuffer* write_buffer) override;

  // virtual SearchStatus FlushBuffer(const std::string* write_buffer) = 0;

  virtual WriteBuffer* NewWriteBuffer(const std::string* write_buffer) override;

  // Release
  virtual void ReleaseWriteBuffer(WriteBuffer* buffer) override;

  // Get one k/v
  virtual SearchStatus Get(StorageColumnType column, const std::string& key,
                           std::string& value,
                           VirtualDBSnapshot* snapshot = nullptr) override;

  // MultiGet will set status's size to keys's size to indicate each key's
  // return status.
  virtual void MultiGet(std::vector<StorageColumnType> columns,
                        std::vector<std::string>& keys,
                        std::vector<std::string>& values,
                        std::vector<SearchStatus>& status,
                        VirtualDBSnapshot* snapshot = nullptr) override;

  // New iterator for read.
  virtual Iterator* NewIterator(StorageColumnType column,
                                VirtualDBReadOption* options) override;

  // Trigger RocksDB Compact one range.
  virtual SearchStatus CompactRange(StorageColumnType column,
                                    const std::string& begin,
                                    const std::string& end) override;

  virtual SearchStatus DropDB() override;

  // Drop rocksdb instance.
  static bool DropDB(const char* path) {
    rocksdb::DestroyDB(path, rocksdb::Options());
  }

  // Test api
  rocksdb::Cache* GetTableCache() {
    rocksdb::DBImpl* db_impl = dynamic_cast<rocksdb::DBImpl*>(db_);
    if (nullptr == db_impl) {
      assert(false);
    }
    return db_impl->TEST_table_cache();
  }

  // Get Rocksdb statistics.
  const std::shared_ptr<rocksdb::Statistics>& GetDBStatistics() const {
    return options_.statistics;
  }

 private:
  void InitDBOptions();

  // new one family
  rocksdb::ColumnFamilyOptions NewColumnFamilyOptions(
      const rocksdb::Options& options, StorageColumnType column);

  // release
  void Clear() {
    for (auto cfh : this->column_famil_handles_) delete cfh;
    this->column_famil_handles_.clear();

    if (nullptr != this->db_) delete this->db_;
    this->db_ = nullptr;
  }
};
}  // namespace wwsearch
