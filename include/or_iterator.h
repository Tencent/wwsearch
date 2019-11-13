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

/* Notice : Get union set from vector<DocIdSetIterator*>
 */
class OrIterator : public DocIdSetIterator {
 private:
  std::vector<DocIdSetIterator*> sub_iterator_;

 public:
  OrIterator() {}

  virtual ~OrIterator() {}

  virtual DocumentID DocID() override;

  virtual DocumentID NextDoc() override;

  virtual DocumentID Advance(DocumentID target) override;

  virtual CostType Cost() override;

  virtual int FieldId() override {
    if (this->sub_iterator_.empty()) return -1;
    return this->sub_iterator_.front()->FieldId();
  };

  void AddSubIterator(DocIdSetIterator* iterator) {
    this->sub_iterator_.push_back(iterator);
  }

  inline void FinishAddIterator() {
    if (!this->sub_iterator_.empty()) {
      std::make_heap(sub_iterator_.begin(), sub_iterator_.end(),
                     OrIterator::IteratorGreater);
    }
  }

  // greater is first.
  static inline bool IteratorGreater(DocIdSetIterator* left,
                                     DocIdSetIterator* right) {
    return left->DocID() < right->DocID();
  }

 private:
  DocumentID InnderNextDoc(bool use_advance = false,
                           DocumentID target = NO_MORE_DOCS);
};
}  // namespace wwsearch
