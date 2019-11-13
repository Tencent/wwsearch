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
#include "scorer.h"
#include "search_context.h"
#include "storage_type.h"

namespace wwsearch {

class Query;
class Scorer;
class BulkScorer;

/* Notice : Really get doc id list from inverted table stored in db.
 * BoolWeight : calling MultiGet method of db
 * PrefixWeight : calling Iterator method of db
 * AndWeight&OrWeight : wrapper for BoolWeigth / PrefixWeight
 * Attention : inverted table key[term] => value[doc_id_list]
 * Terms are passed by user's Query.
 */
class Weight {
 private:
  Query* parent_query_;
  std::string weight_name_;

 public:
  Weight(Query* query, const std::string& weight_name)
      : parent_query_(query), weight_name_(weight_name) {}

  virtual ~Weight() {}

  virtual Scorer* GetScorer(SearchContext* context) = 0;

  inline const std::string& Name() const { return weight_name_; }

  virtual BulkScorer* GetBulkScorer(SearchContext* context) = 0;

  inline Query* GetQuery() { return this->parent_query_; }

 private:
};
}  // namespace wwsearch
