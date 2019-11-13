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

#include "include/codec_impl.h"
#include "include/document.h"
#include "include/index_wrapper.h"
#include "include/index_writer.h"
#include "include/logger.h"
#include "include/virtual_db_rocks.h"

#include "include/and_query.h"
#include "include/bool_query.h"
#include "include/index_wrapper.h"
#include "include/or_query.h"
#include "include/query.h"
#include "include/searcher.h"
#include "include/storage_type.h"
#include "include/tokenizer_impl.h"
#include "include/weight.h"

#include <utility>
#include <vector>
#include "include/logger.h"

extern bool g_debug;
extern bool g_use_rocksdb;
extern bool g_use_compression;

namespace wwsearch {

void InitStringField(IndexField* field, uint16_t field_id,
                     const std::string& word);
void InitUint32Field(IndexField* field, uint16_t field_id, uint32_t value);
void InitUint64Field(IndexField* field, uint16_t field_id, uint64_t value);

class TestUtil {
 public:
  using FieldIdStrPair = std::pair<uint16_t, std::string>;

  static DefaultIndexWrapper* NewIndex(const char* path) {}

  static void DeleteIndex(DefaultIndexWrapper* index) {}

  static DocumentUpdater* NewDocument(DocumentID documentid,
                                      std::string str_field,
                                      uint32_t numeric_32, uint64_t numeric_64,
                                      uint32_t sort_value,
                                      uint8_t field_offset = 0);

  static DocumentUpdater* NewStringFieldDocument(
      DocumentID documentid,
      const std::vector<FieldIdStrPair>& field_id_str_list);
};

}  // namespace wwsearch
