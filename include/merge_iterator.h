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

namespace wwsearch {

/* Notice : Get intersection doc list from vector<DocIdSetIterator*>
 */
class MergeIterator : public DocIdSetIterator {
 private:
  std::vector<DocIdSetIterator*> sub_iterator_;
  DocumentID curr_;
  int field_id_;

 public:
  MergeIterator() : curr_(NO_MORE_DOCS), field_id_(-1) {}

  virtual ~MergeIterator() {}

  virtual DocumentID DocID() override;

  virtual DocumentID NextDoc() override;

  virtual DocumentID Advance(DocumentID target) override;

  virtual CostType Cost() override;

  virtual int FieldId() override { return field_id_; }

  void AddSubIterator(DocIdSetIterator* iterator) {
    this->sub_iterator_.push_back(iterator);
  }

  // must call after AddSubIterator to reach init state.
  inline void FinishAddIterator() { curr_ = Advance(MAX_DOCID); }

 private:
  DocumentID InnderNextDoc(bool use_advance = false,
                           DocumentID target = NO_MORE_DOCS);
};
}  // namespace wwsearch
