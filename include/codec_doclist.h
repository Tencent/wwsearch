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

#include "doc_iterator.h"
#include "search_slice.h"
#include "serialize.h"
#include "storage_type.h"

namespace wwsearch {

// 1 byte in header
// no order insert.
struct DocListHeader {
  // version = 0 -> default,fixed size
  uint8_t version : 4;
  // flag for version used
  uint8_t flag : 3;
  uint8_t extend : 1;
  DocListHeader() : version(0), flag(0), extend(0) {}
} __attribute__((packed));

typedef uint8_t DocumentState;

enum kDocumentIDState { kDocumentStateOK = 0, kDocumentStateDelete = 1 };

class DocListWriterCodec : public SerializeAble {
 public:
  virtual ~DocListWriterCodec() {}

  virtual void AddDocID(const DocumentID& doc_id, DocumentState state) = 0;

  virtual std::string DebugString() = 0;
};

class DocListReaderCodec : public DocIdSetIterator {
 private:
  size_t priority_;  // used for merge.

 public:
  virtual ~DocListReaderCodec() {}

  virtual DocumentState& State() = 0;

  inline void SetPriority(size_t v) { this->priority_ = v; }

  inline size_t GetPriority() { return this->priority_; }

 private:
};

typedef bool (*DocListReaderCodecGreater)(DocListReaderCodec* left,
                                          DocListReaderCodec* right);

}  // namespace wwsearch
