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

#include "and_weight.h"
#include "merge_iterator.h"
#include "scorer.h"

namespace wwsearch {

class AndScorer : public Scorer {
 private:
  std::vector<Scorer *> sub_scorer_;
  MergeIterator iterator_;

 public:
  AndScorer(AndWeight *weight) : Scorer(weight, "AndScorer") {}

  virtual ~AndScorer() {
    // our response to delete scorer
    for (auto s : sub_scorer_) delete s;
  }

  virtual DocumentID DocID() override;

  virtual double Score() override;

  virtual DocIdSetIterator &Iterator() override;

  void AddScorer(Scorer *s) {
    this->sub_scorer_.push_back(s);
    this->iterator_.AddSubIterator(&(s->Iterator()));
  }

 private:
};
}  // namespace wwsearch
