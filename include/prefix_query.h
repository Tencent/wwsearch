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

class PrefixQuery : public Query {
 private:
  // one of two will be used.
  FieldID field_id_;
  std::string match_term_;
  uint32_t max_doc_list_size_;

 public:
  PrefixQuery(FieldID field_id, const std::string &term,
              uint32_t max_doc_list_size = 1000000);

  virtual ~PrefixQuery();

  virtual Weight *CreateWeight(SearchContext *context, bool needs_scores,
                               double boost) override;

  FieldID GetFieldID() { return this->field_id_; }

  inline std::string &MatchTerm() { return this->match_term_; }

  inline uint32_t MaxDocListSize() { return this->max_doc_list_size_; }

 private:
};

}  // namespace wwsearch
