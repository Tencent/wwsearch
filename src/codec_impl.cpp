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

#include "codec_impl.h"
#include "codec_doclist_impl.h"

namespace wwsearch {

CodecImpl::CodecImpl() { this->compression_type_ = DocListCompressionFixType; }

CodecImpl::~CodecImpl() {}

void CodecImpl::EncodeStoredFieldKey(const TableID& table,
                                     const DocumentID& document_id,
                                     std::string& document_key) {
  AppendFixed8(document_key, table.business_type);
  AppendFixed64(document_key, table.partition_set);
  AppendFixed64(document_key, document_id);
}

std::string CodecImpl::DebugStoredFieldKey(const std::string& key) {
  Slice slice(key);

  uint8_t business_type = 0;
  RemoveFixed8(slice, business_type);

  uint64_t partition_set = 0; /* corp id */
  RemoveFixed64(slice, partition_set);

  uint64_t document_id = 0;
  RemoveFixed64(slice, document_id);
  assert(slice.empty() == true);

  char buf[128];
  snprintf(buf, 128,
           "business_type(%d), partition_set/corp_id(%llu), "
           "document_id(%llu)",
           business_type, partition_set, document_id);
  return buf;
}

void CodecImpl::EncodeMetaKey(const TableID& table, std::string& meta_key) {
  AppendFixed8(meta_key, table.business_type);
  AppendFixed64(meta_key, table.partition_set);
}

std::string CodecImpl::DebugMetaKey(const std::string& key) {
  Slice slice(key);

  uint8_t business_type = 0;
  RemoveFixed8(slice, business_type);

  uint64_t partition_set = 0; /* corp id */
  RemoveFixed64(slice, partition_set);

  assert(slice.empty() == true);

  char buf[64];
  snprintf(buf, 64, "business_type(%d), partition_set/corp_id(%llu)",
           business_type, partition_set);
  return buf;
}

void CodecImpl::EncodeInvertedKey(const TableID& table, const FieldID& field_id,
                                  const std::string& term, std::string& key) {
  AppendFixed8(key, table.business_type);
  AppendFixed64(key, table.partition_set);
  assert(sizeof(FieldID) == sizeof(uint8_t));
  AppendFixed8(key, field_id);
  AppendBuffer(key, term.c_str(), term.size());
}

bool CodecImpl::DecodeInvertedKey(const Slice& inverted_key,
                                  uint8_t* business_type,
                                  uint64_t* partition_set /*corp_id*/,
                                  uint8_t* field_id, std::string* term) {
  if (inverted_key.size() < 10) {
    return false;
  }
  Slice key(inverted_key);
  RemoveFixed8(key, *business_type);
  RemoveFixed64(key, *partition_set);
  RemoveFixed8(key, *field_id);
  *term = key.ToString();
  return true;
}

std::string CodecImpl::DebugInvertedKey(const std::string& key) {
  Slice slice(key);
  uint8_t business_type = 0;
  uint64_t partition_set = 0;
  uint8_t field_id = 0;
  std::string term;
  this->DecodeInvertedKey(key, &business_type, &partition_set, &field_id,
                          &term);
  char buf[1024];
  snprintf(buf, 1024,
           "business_type(%d), partition_set/corp_id(%llu), "
           "field_id(%d), term(%s)",
           business_type, partition_set, field_id, term.c_str());
  return buf;
}

DocListWriterCodec* CodecImpl::NewDocListWriterCodec() {
  // not support
  assert(false);
  return nullptr;
  // return new DocListWriterCodecImpl();
}

void CodecImpl::ReleaseDocListWriterCodec(DocListWriterCodec* o) { delete o; }

DocListWriterCodec* CodecImpl::NewOrderDocListWriterCodec() {
  if (this->compression_type_ == DocListCompressionFixType) {
    return new DocListOrderWriterCodecImpl();
  } else if (this->compression_type_ == DocListCompressionVarLenBlockType) {
    return new CompressionWriterCodecImpl();
  }
  assert(false);
  return nullptr;
}

void CodecImpl::ReleaseOrderDocListWriterCodec(DocListWriterCodec* o) {
  delete o;
}

DocListReaderCodec* CodecImpl::NewDocListReaderCodec(const char* data,
                                                     size_t data_len,
                                                     int field_id) {
  return new DocListReaderCodecImpl(data, data_len, field_id);
}

void CodecImpl::ReleaseDocListReaderCodec(DocListReaderCodec* o) { delete o; }

DocListReaderCodecGreater CodecImpl::GetGreaterComparator() {
  return DocListReaderCodecImplGreater;
}

bool CodecImpl::DecodeDocListToFixBytes(const char* data, size_t data_len,
                                        bool& use_buffer, std::string& buffer) {
  if (data_len < sizeof(DocListHeader)) return false;
  DocListHeader header = *(DocListHeader*)data;
  if (header.version == DocListCompressionFixType) {
    use_buffer = false;
    return true;
  } else if (header.version == DocListCompressionVarLenBlockType) {
    DocListCompressionDecoder decoder;

    use_buffer = true;
    bool ret = decoder.Decode(data, data_len, buffer);
    return ret;
  }

  return false;
}

void CodecImpl::SetDocListCompressionType(DocListCompressionType type) {
  this->compression_type_ = type;
}

void CodecImpl::EncodeSequenceMetaKey(const TableID& table, std::string& key) {
  AppendFixed8(key, table.business_type);
  AppendFixed64(key, table.partition_set);
  AppendFixed8(key, 0);
}

void CodecImpl::EncodeSequenceMappingKey(const TableID& table,
                                         std::string& user_id,
                                         std::string& key) {
  AppendFixed8(key, table.business_type);
  AppendFixed64(key, table.partition_set);
  AppendFixed8(key, 1);
  key.append(user_id);
}

}  // namespace wwsearch
