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

#include "codec_doclist.h"
#include "coding.h"
#include "doclist_compression.h"
#include "header.h"
#include "search_slice.h"
#include "storage_type.h"

namespace wwsearch {

/* Notice : This Codec staff aims to work for forward,inverted table.
 * All key&value encode/decode depends on this class.
 * Besides, inverted table 's dos_list support fix-length format and
 * compression format.
 * You could find more information in codec_impl.h
 */
class Codec {
 private:
 public:
  virtual ~Codec();

  // Stored field
  virtual void EncodeStoredFieldKey(const TableID& table,
                                    const DocumentID& document_id,
                                    std::string& document_key) = 0;
  virtual std::string DebugStoredFieldKey(const std::string& key) = 0;

  // Invert index
  virtual void EncodeInvertedKey(const TableID& table, const FieldID& field_id,
                                 const std::string& term, std::string& key) = 0;
  virtual bool DecodeInvertedKey(const Slice& inverted_key,
                                 uint8_t* business_type,
                                 uint64_t* partition_set /*corp_id*/,
                                 uint8_t* field_id, std::string* term) = 0;
  virtual std::string DebugInvertedKey(const std::string& key) = 0;

  // write
  virtual DocListWriterCodec* NewDocListWriterCodec() = 0;

  virtual void ReleaseDocListWriterCodec(DocListWriterCodec*) = 0;

  virtual DocListWriterCodec* NewOrderDocListWriterCodec() = 0;

  virtual void ReleaseOrderDocListWriterCodec(DocListWriterCodec*) = 0;

  // read
  virtual DocListReaderCodec* NewDocListReaderCodec(const char* data,
                                                    size_t data_len,
                                                    int field_id = -1) = 0;

  virtual void ReleaseDocListReaderCodec(DocListReaderCodec*) = 0;

  virtual DocListReaderCodecGreater GetGreaterComparator() = 0;

  // Try to decode doclist to fix array
  // If success,return true
  //     If input data is fix array,use_buffer will set to false and will not
  //     set buffer. if input data is varint encoding,use_buffer will set to
  //     true and will set to buffer.
  virtual bool DecodeDocListToFixBytes(const char* data, size_t data_len,
                                       bool& use_buffer,
                                       std::string& buffer) = 0;
  // Doc value

  // Meta
  virtual void EncodeMetaKey(const TableID& table, std::string& meta_key) = 0;
  virtual std::string DebugMetaKey(const std::string& key) = 0;
  virtual void EncodeSequenceMetaKey(
      const TableID& table, std::string& meta_key) = 0;  // store max sequence
  virtual void EncodeSequenceMappingKey(
      const TableID& table, std::string& user_id,
      std::string& meta_key) = 0;  // store user's key mapping to docid

  // Dictionary

  // DocListCompression
  virtual void SetDocListCompressionType(DocListCompressionType type) = 0;

 private:
};

}  // namespace wwsearch
