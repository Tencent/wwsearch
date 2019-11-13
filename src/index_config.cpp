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

#include "index_config.h"

namespace wwsearch {

IndexConfig::IndexConfig()
    : codec_(nullptr), vdb_(nullptr), tokenizer_(nullptr) {
  this->log_level_ = kSearchLogLevelError;
}

IndexConfig::~IndexConfig() {}

bool IndexConfig::SetCodec(wwsearch::Codec* codec) {
  if (nullptr != this->codec_) return false;
  this->codec_ = codec;
  return true;
}

Codec* IndexConfig::GetCodec() { return this->codec_; }

bool IndexConfig::SetVirtualDB(VirtualDB* vdb) {
  if (nullptr != this->vdb_) return false;
  this->vdb_ = vdb;
  return true;
}

VirtualDB* IndexConfig::VDB() { return this->vdb_; }

bool IndexConfig::SetTokenizer(Tokenizer* tokenizer) {
  if (nullptr == tokenizer) return false;
  this->tokenizer_ = tokenizer;
  return true;
}

Tokenizer* IndexConfig::GetTokenizer() { return this->tokenizer_; }

}  // namespace wwsearch
