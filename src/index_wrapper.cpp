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

#include "index_wrapper.h"
#include "logger.h"
#include "search_util.h"
#include "tokenizer_mmseg.h"
#include "virtual_db_mock.h"
#include "virtual_db_rocks.h"

namespace wwsearch {

SearchStatus DefaultIndexWrapper::Open(bool use_rocksdb, bool use_compression) {
  SearchStatus status;
  DefaultLoggerImpl *logger = new DefaultLoggerImpl;
  Logger::SetLogger(logger, this->config_.GetLogLevel());
  // vdb params
  {
    codec_ = new CodecImpl();
    if (use_compression) {
      codec_->SetDocListCompressionType(DocListCompressionVarLenBlockType);
    }
    this->params_.codec_ = codec_;
  }
  if (vdb_ == nullptr) {
    if (!use_rocksdb) {
      vdb_ = new VirtualDBMock(codec_);
    } else {
      vdb_ = new VirtualDBRocksImpl(&this->params_, nullptr);
    }
    SearchLogInfo("%s", use_rocksdb ? "use_rocksdb" : "not use_rocksdb");
  }
  if (!vdb_->Open()) {
    status.SetStatus(kVirtualDBOpenErrorStatus, vdb_->GetState().c_str());
    SearchLogError("open db error %s", vdb_->GetState().c_str());
    return status;
  }

  // index config
  {
    tokenizer_ = new TokenizerMMSEG(
        params_.mmseg_dict_dir_.size() == 0 ? "./"
                                            : params_.mmseg_dict_dir_.c_str(),
        params_.mmseg_instance_num_ == 0 ? 10 : params_.mmseg_instance_num_);
  }
  config_.SetCodec(this->params_.codec_);  // deprecated set
  config_.SetVirtualDB(vdb_);
  config_.SetTokenizer(tokenizer_);

  // open index writer
  index_writer_ = new IndexWriter();
  if (!index_writer_->Open(&config_)) {
    status.SetStatus(kIndexWriterOpenErrorStatus, "open index writer fail");
    SearchLogError("open index writer fail");
    return status;
  }

  return status;
}

DefaultIndexWrapper::~DefaultIndexWrapper() {
  DELETE_RESET(codec_);
  DELETE_RESET(vdb_);
  DELETE_RESET(tokenizer_);
  DELETE_RESET(index_writer_);
}

}  // namespace wwsearch
