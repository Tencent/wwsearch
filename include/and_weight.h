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

#include "and_query.h"
#include "weight.h"

namespace wwsearch {

class AndWeight : public Weight {
 private:
  std::vector<Weight*> sub_weight_;

 public:
  AndWeight(AndQuery* query) : Weight(query, "AndWeight") {}

  virtual ~AndWeight() {
    // our response to delete sub weight
    for (auto w : sub_weight_) {
      delete w;
    }
  }

  virtual Scorer* GetScorer(SearchContext* context) override;

  virtual BulkScorer* GetBulkScorer(SearchContext* context) override;

  void AddWeight(Weight* w) { this->sub_weight_.push_back(w); }

 private:
};
}  // namespace wwsearch
