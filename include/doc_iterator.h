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

#include "header.h"
#include "storage_type.h"

namespace wwsearch {

/* Notice : Helper class for doc list iterator.
 * Support MergeIterator/OrIterator/DocListReaderCodec.
 * MergeIterator/OrIterator is used in Score to collect doc list.
 * DocListReaderCodec is used to read the doc list after codec(both
 * support fix-length format & variable format, distinguish with header version
 * flag).
 */
class DocIdSetIterator {
 private:
 public:
  virtual ~DocIdSetIterator() {}

  static DocumentID NO_MORE_DOCS;

  static DocumentID MAX_DOCID;

  virtual DocumentID DocID() = 0;

  // If reac end {@link NO_MORE_DOCS} will return.
  virtual DocumentID NextDoc() = 0;

  // Seek to target or next one greater than target if target is not found.
  virtual DocumentID Advance(DocumentID target) = 0;

  virtual CostType Cost() = 0;

  virtual int FieldId() = 0;

 private:
};

}  // namespace wwsearch
