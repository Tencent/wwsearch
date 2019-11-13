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
#include "codec_impl.h"
#include "storage_type.h"
#include "virtual_db.h"

namespace wwsearch {
class VirtualDBMock : public VirtualDB {
 private:
  CodecImpl* codec_;
  std::map<StorageColumnType, std::function<std::string(const std::string&)>>
      cf_debug_string_funcs_;

  using KvList = std::map<std::string, std::string>;
  std::map<StorageColumnType, KvList> cf_kv_list_;
  std::mutex mut_;

 public:
  VirtualDBMock(CodecImpl* codec);

  virtual ~VirtualDBMock() {}

  virtual bool Open() override;

  virtual VirtualDBSnapshot* NewSnapshot() override;

  virtual void ReleaseSnapshot(VirtualDBSnapshot*) override;

  virtual SearchStatus FlushBuffer(WriteBuffer* write_buffer) override;

  virtual WriteBuffer* NewWriteBuffer(const std::string* write_buffer) override;

  virtual void ReleaseWriteBuffer(WriteBuffer* buffer) override;

  virtual SearchStatus Get(StorageColumnType column, const std::string& key,
                           std::string& value,
                           VirtualDBSnapshot* snapshot) override;

  virtual void MultiGet(std::vector<StorageColumnType> columns,
                        std::vector<std::string>& keys,
                        std::vector<std::string>& values,
                        std::vector<SearchStatus>& status,
                        VirtualDBSnapshot* snapshot) override;

  virtual Iterator* NewIterator(StorageColumnType column,
                                VirtualDBReadOption* options) override;

  virtual SearchStatus CompactRange(StorageColumnType column,
                                    const std::string& begin,
                                    const std::string& end) override;

  virtual SearchStatus DropDB() override;

 private:
  SearchStatus ParsePbString(const std::string& write_buffer_data);
};
}  // namespace wwsearch
