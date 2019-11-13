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

#include "or_query.h"
#include "or_weight.h"

namespace wwsearch {

Weight* OrQuery::CreateWeight(SearchContext* context, bool needs_scores,
                              double boost) {
  OrWeight* weight = new OrWeight(this);
  for (auto q : this->sub_query_) {
    auto w = q->CreateWeight(context, needs_scores, 0);
    weight->AddWeight(w);
  }
  return weight;
}

}  // namespace wwsearch
