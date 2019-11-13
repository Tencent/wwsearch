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

#include "merge_iterator.h"
#include "logger.h"

namespace wwsearch {

DocumentID MergeIterator::DocID() { return curr_; }

DocumentID MergeIterator::NextDoc() { return InnderNextDoc(false, 0); }

DocumentID MergeIterator::Advance(DocumentID target) {
  return InnderNextDoc(true, target);
}

CostType MergeIterator::Cost() {
  // not support now
  assert(false);
  return 0;
}

DocumentID MergeIterator::InnderNextDoc(bool use_advance, DocumentID target) {
  for (auto iterator : this->sub_iterator_) {
    if (use_advance)
      iterator->Advance(target);
    else
      iterator->NextDoc();
  }

  for (;;) {
    bool first = true, retry = false;
    // find min id
    DocumentID min = NO_MORE_DOCS;
    FieldID new_field_id = 0;
    for (auto iterator : this->sub_iterator_) {
      DocumentID docid = iterator->DocID();
      FieldID fieldid = iterator->FieldId();
      SearchLogDebug(
          "InnderNextDoc docid=%llu, min=%llu, filedid=%u, new_field_id=%u",
          docid, min, fieldid, new_field_id);
      if (first) {
        min = docid;
        new_field_id = fieldid;
        first = false;
      } else if (docid < min) {
        min = docid;
        new_field_id = fieldid;
        retry = true;
      } else if (docid > min) {
        retry = true;
      }
    }

    // all head id is same
    if (!retry) {
      curr_ = min;
      field_id_ = new_field_id;
      SearchLogDebug("Finally break, InnderNextDoc curr=%llu field_id=%u",
                     curr_, field_id_);
      break;
    }

    SearchLogDebug("merge Advance min=%llu", min);
    // advance all
    for (auto iterator : this->sub_iterator_) {
      SearchLogDebug("before merge Advance docid=%llu, min=%llu",
                     iterator->DocID(), min);
      if (iterator->DocID() > min) {
        iterator->Advance(min);
        SearchLogDebug("after merge Advance docid=%llu, min=%llu",
                       iterator->DocID(), min);
      }
    }
  }

  return curr_;
}

}  // namespace wwsearch
