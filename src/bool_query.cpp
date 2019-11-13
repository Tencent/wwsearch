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

#include "bool_query.h"
#include "bool_weight.h"

namespace wwsearch {

BooleanQuery::BooleanQuery(FieldID field_id, const std::string &term)
    : value_type_(kStringIndexField), field_id_(field_id), match_term_(term) {}

BooleanQuery::BooleanQuery(FieldID field_id, uint64_t value)
    : value_type_(kUint64IndexField),
      field_id_(field_id),
      match_numeric_(value) {}

BooleanQuery::BooleanQuery(FieldID field_id, uint32_t value)
    : value_type_(kUint32IndexField),
      field_id_(field_id),
      match_numeric_(value) {}

BooleanQuery::~BooleanQuery() {}

Weight *BooleanQuery::CreateWeight(SearchContext *context, bool needs_scores,
                                   double boost) {
  return new BooleanWeight(this);
}

}  // namespace wwsearch
