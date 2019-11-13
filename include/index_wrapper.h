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

#include "codec_impl.h"
#include "document.h"
#include "index_writer.h"
#include "logger.h"
#include "search_status.h"

#include "bool_query.h"
#include "query.h"
#include "searcher.h"
#include "storage_type.h"
#include "tokenizer_impl.h"
#include "weight.h"

namespace wwsearch {

/* A simple wrapper use default params.
 * Only use in internal test.
 */
class DefaultIndexWrapper {
 public:
  VDBParams params_;
  CodecImpl *codec_;
  VirtualDB *vdb_;

  Tokenizer *tokenizer_;

  IndexConfig config_;
  IndexWriter *index_writer_;

 public:
  DefaultIndexWrapper() {
    codec_ = nullptr;
    vdb_ = nullptr;
    tokenizer_ = nullptr;
    index_writer_ = nullptr;
  }

  void SetVdb(VirtualDB *vdb) { vdb_ = vdb; }

  VDBParams &DBParams() { return this->params_; }

  IndexConfig &Config() { return this->config_; }

  virtual ~DefaultIndexWrapper();

  SearchStatus Open(bool use_rocksdb = false, bool use_compression = false);

 private:
};

}  // namespace wwsearch
