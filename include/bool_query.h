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

#include "header.h"
#include "query.h"

namespace wwsearch {

class BooleanQuery : public Query {
 private:
  // one of two will be used.
  FieldID field_id_;
  kIndexFieldType value_type_;
  std::string match_term_;
  uint64_t match_numeric_;

 public:
  BooleanQuery(FieldID field_id, const std::string &term);

  BooleanQuery(FieldID field_id, uint64_t value);

  BooleanQuery(FieldID field_id, uint32_t value);

  virtual ~BooleanQuery();

  virtual Weight *CreateWeight(SearchContext *context, bool needs_scores,
                               double boost) override;

  FieldID GetFieldID() { return this->field_id_; }

  inline kIndexFieldType ValueType() { return this->value_type_; }

  inline bool IsTerm() { return this->value_type_ == kStringIndexField; }

  inline std::string &MatchTerm() { return this->match_term_; }

  inline uint64_t MatchNumeric() { return this->match_numeric_; }

 private:
};

}  // namespace wwsearch
