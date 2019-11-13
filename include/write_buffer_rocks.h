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

#include "write_buffer.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

namespace wwsearch {

class WriteBufferRocksImpl : public WriteBuffer {
 private:
  rocksdb::WriteBatch* write_batch_;
  std::vector<rocksdb::ColumnFamilyHandle*>* column_family_;
  rocksdb::WriteOptions* write_options_;
  // std::atomic<uint64_t> kv_count_;

 public:
  WriteBufferRocksImpl(std::vector<rocksdb::ColumnFamilyHandle*>* column_family,
                       rocksdb::WriteOptions* write_options,
                       const std::string* write_buffer);

  WriteBufferRocksImpl(const WriteBufferRocksImpl& write_buffer_rocks_impl) {
    *this = write_buffer_rocks_impl;
  }

  WriteBufferRocksImpl& operator=(
      const WriteBufferRocksImpl& write_buffer_rocks_impl) {
    this->write_batch_ =
        new rocksdb::WriteBatch(*(write_buffer_rocks_impl.write_batch_));
    this->column_family_ = write_buffer_rocks_impl.column_family_;
    this->write_options_ = write_buffer_rocks_impl.write_options_;
  }

  virtual ~WriteBufferRocksImpl();

  virtual SearchStatus Put(StorageColumnType column, const std::string& key,
                           const std::string& value) override;

  virtual SearchStatus Delete(StorageColumnType column,
                              const std::string& key) override;

  virtual SearchStatus DeleteRange(StorageColumnType column,
                                   const std::string& begin_key,
                                   const std::string& end_key) override;

  virtual SearchStatus Merge(StorageColumnType column, const std::string& key,
                             const std::string& value) override;

  virtual SearchStatus Append(WriteBuffer* prepare_append_write_buffer);

  virtual std::string Data() override;

  virtual uint64_t DataSize() const override;

  virtual uint64_t KvCount() const override;

  rocksdb::WriteBatch* GetWriteBatch();

 private:
  friend class VirtualDBRocksImpl;
};

}  // namespace wwsearch
