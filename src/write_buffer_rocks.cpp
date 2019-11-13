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

#include "write_buffer_rocks.h"
#include "db/write_batch_internal.h"

namespace wwsearch {

WriteBufferRocksImpl::WriteBufferRocksImpl(
    std::vector<rocksdb::ColumnFamilyHandle*>* column_family,
    rocksdb::WriteOptions* write_options, const std::string* write_buffer)
    : column_family_(column_family), write_options_(write_options) {
  // recover from  previous buffer
  // WARNING: write_buffer must be a valid RocksDB::WriteBatch
  // Other rocksdb will coruption.
  if (nullptr != write_buffer && !write_buffer->empty()) {
    this->write_batch_ = new rocksdb::WriteBatch(*write_buffer);
  } else {
    this->write_batch_ = new rocksdb::WriteBatch();
  }
  assert(column_family_->size() == kMaxColumn);
}

WriteBufferRocksImpl::~WriteBufferRocksImpl() {
  if (nullptr != this->write_batch_) {
    delete this->write_batch_;
    this->write_batch_ = nullptr;
  }
}

SearchStatus WriteBufferRocksImpl::Put(StorageColumnType column,
                                       const std::string& key,
                                       const std::string& value) {
  SearchStatus status;
  auto s = this->write_batch_->Put((*column_family_)[column], key, value);
  if (!s.ok()) {
    status.SetStatus(kRocksDBErrorStatus, s.getState());
  }
  return status;
}

SearchStatus WriteBufferRocksImpl::Delete(StorageColumnType column,
                                          const std::string& key) {
  SearchStatus status;

  auto s = this->write_batch_->Delete((*column_family_)[column], key);
  if (!s.ok()) {
    status.SetStatus(kRocksDBErrorStatus, s.getState());
  }
  return status;
}

SearchStatus WriteBufferRocksImpl::DeleteRange(StorageColumnType column,
                                               const std::string& begin_key,
                                               const std::string& end_key) {
  SearchStatus status;
  auto s = this->write_batch_->DeleteRange((*column_family_)[column], begin_key,
                                           end_key);
  if (!s.ok()) {
    status.SetStatus(kRocksDBErrorStatus, s.getState());
  }
  return status;
}

SearchStatus WriteBufferRocksImpl::Merge(StorageColumnType column,
                                         const std::string& key,
                                         const std::string& value) {
  SearchStatus status;
  auto s = this->write_batch_->Merge((*column_family_)[column], key, value);
  if (!s.ok()) {
    status.SetStatus(kRocksDBErrorStatus, s.getState());
  }
  return status;
}

SearchStatus WriteBufferRocksImpl::Append(
    WriteBuffer* prepare_append_write_buffer) {
  SearchStatus status;
  WriteBufferRocksImpl* write_buffer =
      dynamic_cast<WriteBufferRocksImpl*>(prepare_append_write_buffer);
  assert(write_buffer != nullptr);
  auto s = rocksdb::WriteBatchInternal::Append(
      this->write_batch_, write_buffer->write_batch_, false);
  if (!s.ok()) {
    status.SetStatus(kRocksDBErrorStatus, s.getState());
  }
  return status;
}

rocksdb::WriteBatch* WriteBufferRocksImpl::GetWriteBatch() {
  return write_batch_;
};

std::string WriteBufferRocksImpl::Data() { return this->write_batch_->Data(); }

uint64_t WriteBufferRocksImpl::DataSize() const {
  return this->write_batch_->GetDataSize();
}

uint64_t WriteBufferRocksImpl::KvCount() const {
  return this->write_batch_->Count();
}

}  // namespace wwsearch
