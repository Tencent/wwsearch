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

#include <map>
#include <mutex>
#include <string>
#include "codec_impl.h"
#include "search_store.pb.h"
#include "write_buffer.h"

namespace wwsearch {

class WriteBufferMock : public WriteBuffer {
 private:
  uint64_t kv_cnt_{0};
  uint32_t total_size_{0};
  using KvList =
      std::map<std::string, std::pair<std::string, lsmsearch::MockData_Type>>;
  std::map<StorageColumnType, KvList> cf_kv_list_;
  using DeleteKeyRangeList = std::set<std::pair<std::string, std::string>>;
  std::map<StorageColumnType, DeleteKeyRangeList> cf_delete_keyrange_list_;
  CodecImpl* codec_;
  std::string data_;
  std::mutex mut_;

 public:
  // constructor
  WriteBufferMock(CodecImpl* codec, const std::string& data);
  WriteBufferMock(CodecImpl* codec) : codec_(codec) {}

  virtual ~WriteBufferMock() {}
  // Put
  virtual SearchStatus Put(StorageColumnType column, const std::string& key,
                           const std::string& value) override;

  // Delete
  virtual SearchStatus Delete(StorageColumnType column,
                              const std::string& key) override;

  // DeleteRange in db
  virtual SearchStatus DeleteRange(StorageColumnType column,
                                   const std::string& begin_key,
                                   const std::string& end_key) override;
  // Merge
  virtual SearchStatus Merge(StorageColumnType column, const std::string& key,
                             const std::string& value) override;

  // Append WriteBuffer to db
  virtual SearchStatus Append(
      WriteBuffer* prepare_append_write_buffer) override;

  // Return Current data
  virtual std::string Data() override;

  // Return byte size of used.
  virtual uint64_t DataSize() const override;

  // Return k/v number in db.
  virtual uint64_t KvCount() const override;

 private:
  // Inner serialize api
  SearchStatus SeriableData();
  SearchStatus DeSeriableData();
};
}  // namespace wwsearch
