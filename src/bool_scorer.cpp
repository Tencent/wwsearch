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

#include "bool_scorer.h"

namespace wwsearch {

BooleanScorer::~BooleanScorer() {
  if (nullptr != iterator_) {
    codec_->ReleaseDocListReaderCodec((DocListReaderCodec *)iterator_);
    this->iterator_ = nullptr;
  }
}

DocumentID BooleanScorer::DocID() { return this->iterator_->DocID(); }

double BooleanScorer::Score() {
  // If support score, use DocID() to compute doc score.
  assert(false);
  return 0;
}

DocIdSetIterator &BooleanScorer::Iterator() { return *(this->iterator_); }

}  // namespace wwsearch
