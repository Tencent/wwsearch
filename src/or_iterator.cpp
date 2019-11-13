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

#include "or_iterator.h"
#include "header.h"
#include "logger.h"

namespace wwsearch {

DocumentID OrIterator::DocID() {
  if (this->sub_iterator_.empty()) return NO_MORE_DOCS;
  return this->sub_iterator_.front()->DocID();
}

DocumentID OrIterator::NextDoc() {
  DocumentID curr = DocID();
  if (NO_MORE_DOCS == curr) return NO_MORE_DOCS;
  return InnderNextDoc(false, curr - 1);
}

DocumentID OrIterator::Advance(DocumentID target) {
  return InnderNextDoc(true, target);
}

CostType OrIterator::Cost() {
  // not support now
  assert(false);
  return 0;
}

DocumentID OrIterator::InnderNextDoc(bool use_advance, DocumentID target) {
  if (this->sub_iterator_.empty()) {
    return NO_MORE_DOCS;
  }

  SearchLogDebug("before OrIterator target=%llu, DocID=%llu", target,
                 sub_iterator_.front()->DocID());
  for (;;) {
    DocumentID curr = sub_iterator_.front()->DocID();
    if (curr != NO_MORE_DOCS && curr > target) {
      std::pop_heap(sub_iterator_.begin(), sub_iterator_.end(),
                    OrIterator::IteratorGreater);
      // seek the first to smaller one
      if (use_advance)
        sub_iterator_.back()->Advance(target);
      else
        sub_iterator_.back()->NextDoc();
      std::push_heap(sub_iterator_.begin(), sub_iterator_.end(),
                     OrIterator::IteratorGreater);
    } else {
      break;
    }
  }
  SearchLogDebug("after OrIterator DocID=%llu", sub_iterator_.front()->DocID());
  return sub_iterator_.front()->DocID();
}

}  // namespace wwsearch
