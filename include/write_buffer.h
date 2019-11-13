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

#include "search_status.h"
#include "storage_type.h"

namespace wwsearch {

class WriteBuffer {
 public:
  virtual ~WriteBuffer();
  // Put
  virtual SearchStatus Put(StorageColumnType column, const std::string& key,
                           const std::string& value) = 0;

  // Delete
  virtual SearchStatus Delete(StorageColumnType column,
                              const std::string& key) = 0;

  // DeleteRange ["begin_key", "end_key")
  virtual SearchStatus DeleteRange(StorageColumnType column,
                                   const std::string& begin_key,
                                   const std::string& end_key) = 0;

  // Merge
  virtual SearchStatus Merge(StorageColumnType column, const std::string& key,
                             const std::string& value) = 0;

  virtual SearchStatus Append(WriteBuffer* prepare_append_write_buffer) = 0;

  virtual std::string Data() = 0;

  virtual uint64_t DataSize() const = 0;

  virtual uint64_t KvCount() const = 0;

 private:
};
}  // namespace wwsearch
