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

#include <memory>
#include "codec.h"
#include "codec_doclist.h"
#include "doclist_compression.h"
#include "storage_type.h"

namespace wwsearch {

typedef struct DocListHeader DocListHeader;

// Attetion : use this DocListWriterCodec must insert DocID in order.
class DocListOrderWriterCodecImpl : public DocListWriterCodec {
 private:
  std::unique_ptr<std::string> data_;

 public:
  DocListOrderWriterCodecImpl() : data_(new std::string) {
    DocListHeader header;
    assert(sizeof(header) == 1);
    AppendBuffer(*data_, (char*)&header, sizeof(header));
  }

  virtual ~DocListOrderWriterCodecImpl() {}

  // Must keep order doc_id < previous doc_id which added
  virtual void AddDocID(const DocumentID& doc_id, DocumentState state) override;

  virtual std::string DebugString() override;

  virtual bool SerializeToBytes(std::string& buffer, int mode = 0) override;

  virtual bool DeSerializeFromByte(const char* buffer,
                                   uint32_t buffer_len) override;

 private:
};

// Attetion : use this DocListWriterCodec must insert DocID in order.
class CompressionWriterCodecImpl : public DocListWriterCodec {
 private:
  DocListCompressionEncoder encoder_;

 public:
  CompressionWriterCodecImpl() {}

  virtual ~CompressionWriterCodecImpl() {}

  // Must keep order doc_id < previous doc_id which added
  virtual void AddDocID(const DocumentID& doc_id, DocumentState state) override;

  virtual std::string DebugString() {
    // not support
    return std::string("NOT SUPPORT COMPRESSION ENCODED!!!");
  }

  virtual bool SerializeToBytes(std::string& buffer, int mode = 0) override;

  virtual bool DeSerializeFromByte(const char* buffer, uint32_t buffer_len) {
    assert(false);
    return false;
  }

 private:
};

class CodecImpl;

class DocListReaderCodecImpl : public DocListReaderCodec {
 private:
  Slice slice_;
  size_t pos_;

  // for compression
  std::string buffer_;

  int field_id_;

 public:
  friend class CodecImpl;
  friend class DocListOrderWriterCodecImpl;

  virtual ~DocListReaderCodecImpl();

  virtual DocumentID DocID() override;

  virtual DocumentID NextDoc() override;

  virtual DocumentID Advance(DocumentID target) override;

  virtual CostType Cost() override;

  virtual DocumentState& State() override;

  virtual int FieldId() override { return field_id_; };

 private:
  DocListReaderCodecImpl(const char* data, size_t data_len, int field_id = -1);
};

// Attetion : must place doc list in decrease order
inline bool DocListReaderCodecImplGreater(DocListReaderCodec* left,
                                          DocListReaderCodec* right) {
  assert(left->GetPriority() != right->GetPriority());
  if (left->DocID() > right->DocID())
    return false;
  else if (left->DocID() < right->DocID())
    return true;
  return left->GetPriority() < right->GetPriority();
}

}  // namespace wwsearch
