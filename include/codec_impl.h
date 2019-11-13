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

namespace wwsearch {

/* Notice : This Codec staff aims to work for forward, inverted, docvalue table.
 *
 * 1. In forward table, the data looks like below :
 *            key                      |           value
 *       table_id[bussiness_type,      |     document lsmstore pb
 *         partition_set],document_id  |(only including store flag field)
 *
 * 2. In inverted table, the data looks like :
 *            key                      |           value
 *       table_id[bussiness_type,      |
 *       partition_set],field_id,term  |   doc_id3,doc_id2,doc_id1
 *
 * 3. In docvalue table, the data looks like :
 *            key                      |           value
 *       table_id[bussiness_type,      |     document lsmstore pb
 *       partition_set],document_id    |(only including docvalue flag fields)
 *
 *
 * All key&value encode/decode depends on this class.
 *
 * Besides, inverted table 's dos_list support fix-length format and
 * compression format.
 *
 * 1. Fix-length format looks likes:
 * [header(1B)][doc_id(8B)][doc_state(1B)]...[doc_id(8B)][doc_state(1B)]
 * doc_id is uint64_t
 * doc_state is uint8_t which stands for Add/Delete
 *
 * 2. Compression format
 * [header(1B)][delete flag block][(doc_id,doc_state) variable length]
 *   ...[(doc_id,doc_state) variable length]
 * Use delta compression at the same time.
 */
class CodecImpl : public Codec {
 private:
  DocListCompressionType compression_type_;

 public:
  CodecImpl();

  virtual ~CodecImpl();

  virtual void EncodeStoredFieldKey(const TableID& table,
                                    const DocumentID& document_id,
                                    std::string& document_key) override;
  virtual std::string DebugStoredFieldKey(const std::string& key) override;

  virtual void EncodeMetaKey(const TableID& table,
                             std::string& meta_key) override;
  virtual std::string DebugMetaKey(const std::string& key) override;

  virtual void EncodeInvertedKey(const TableID& table, const FieldID& field_id,
                                 const std::string& term,
                                 std::string& key) override;
  virtual bool DecodeInvertedKey(const Slice& inverted_key,
                                 uint8_t* business_type,
                                 uint64_t* partition_set /*corp_id*/,
                                 uint8_t* field_id, std::string* term) override;

  virtual std::string DebugInvertedKey(const std::string& key) override;

  virtual DocListWriterCodec* NewDocListWriterCodec() override;

  virtual void ReleaseDocListWriterCodec(DocListWriterCodec*) override;

  virtual DocListWriterCodec* NewOrderDocListWriterCodec() override;

  virtual void ReleaseOrderDocListWriterCodec(DocListWriterCodec*) override;

  virtual DocListReaderCodec* NewDocListReaderCodec(const char* data,
                                                    size_t data_len,
                                                    int field_id = -1) override;

  virtual void ReleaseDocListReaderCodec(DocListReaderCodec*) override;

  virtual DocListReaderCodecGreater GetGreaterComparator() override;

  virtual bool DecodeDocListToFixBytes(const char* data, size_t data_len,
                                       bool& use_buffer,
                                       std::string& buffer) override;

  virtual void SetDocListCompressionType(DocListCompressionType type) override;

  virtual void EncodeSequenceMetaKey(const TableID& table,
                                     std::string& meta_key) override;

  virtual void EncodeSequenceMappingKey(const TableID& table,
                                        std::string& user_id,
                                        std::string& meta_key) override;

 private:
};

}  // namespace wwsearch
