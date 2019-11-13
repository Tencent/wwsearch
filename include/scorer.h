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
#include "storage_type.h"
#include "weight.h"

namespace wwsearch {
class Weight;

/* Notice : The interface is used to get DocID list.
 * Input : Weight
 * Output : Doc id list
 * Getting doc id list by calling weight.
 * This `Scorer` class supports extending to score the doc id output or not.
 */
class Scorer {
 private:
  Weight *weight_;
  std::string scorer_name_;

 public:
  Scorer(Weight *weight, const std::string &scorer_name)
      : weight_(weight), scorer_name_(scorer_name) {}

  virtual ~Scorer() {}

  virtual DocumentID DocID() = 0;

  virtual double Score() = 0;

  inline const std::string &Name() const { return scorer_name_; }

  virtual DocIdSetIterator &Iterator() = 0;

  inline Weight *GetWeight() { return this->weight_; }

 private:
};

class BulkScorer {
 private:
 public:
  virtual ~BulkScorer() {}

 private:
};

}  // namespace wwsearch
