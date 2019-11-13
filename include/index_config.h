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
#include "header.h"
#include "tokenizer.h"
#include "virtual_db.h"

namespace wwsearch {

class IndexConfig {
 private:
  Codec* codec_;
  VirtualDB* vdb_;
  Tokenizer* tokenizer_;
  uint32_t min_suffix_build_len_{5};         // min suffix build len.Default:5
  uint32_t max_suffix_term_size_{64};        // max suffix term size
  uint32_t max_write_batch_size_{19922944};  // max write batch size limit: 19MB
  uint32_t max_inner_purge_batch_docs_count_{
      10000};  // max batch docs in InnerPurge
  uint32_t max_inner_purge_docs_total_limit_{50000};
  SearchLogLevel log_level_;

 public:
  IndexConfig();

  ~IndexConfig();

  bool SetCodec(Codec* codec);

  Codec* GetCodec();

  bool SetVirtualDB(VirtualDB* vdb);

  VirtualDB* VDB();

  bool SetTokenizer(Tokenizer* tokenizer);

  Tokenizer* GetTokenizer();

  bool SetMinSuffixBuildLen(uint32_t min_len) {
    this->min_suffix_build_len_ = min_len;
  }

  uint32_t GetMinSuffixBuildLen() { return this->min_suffix_build_len_; }

  bool SetMaxSuffixTermSize(uint32_t max_suffix_term_size) {
    this->max_suffix_term_size_ = max_suffix_term_size;
  }

  uint32_t GetMaxSuffixTermSize() { return this->max_suffix_term_size_; }

  bool SetMaxWriteBatchSize(uint32_t max_write_batch_size) {
    this->max_write_batch_size_ = max_write_batch_size;
  }

  uint32_t GetMaxWriteBatchSize() { return this->max_write_batch_size_; }

  bool SetMaxInnerPurgeBatchDocsCount(
      uint32_t max_inner_purge_batch_docs_count) {
    this->max_inner_purge_batch_docs_count_ = max_inner_purge_batch_docs_count;
  }

  uint32_t GetMaxInnerPurgeBatchDocsCount() {
    return this->max_inner_purge_batch_docs_count_;
  }

  bool SetMaxInnerPurgeDocsTotalLimit(
      uint32_t max_inner_purge_docs_total_limit) {
    this->max_inner_purge_docs_total_limit_ = max_inner_purge_docs_total_limit;
  }

  uint32_t GetMaxInnerPurgeDocsTotalLimit() {
    return this->max_inner_purge_docs_total_limit_;
  }

  bool SetLogLevel(SearchLogLevel log_level) {
    this->log_level_ = log_level;
    return true;
  }

  SearchLogLevel GetLogLevel() { return this->log_level_; }

 private:
};
}  // namespace wwsearch
