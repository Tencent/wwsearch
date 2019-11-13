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

#include "filter.h"
#include "index_config.h"
#include "post_scorer.h"
#include "query.h"
#include "search_status.h"
#include "sorter.h"
#include "tracer.h"

namespace wwsearch {

/* Notice : Query interface for user.
 * input : Query, filters, sorters
 * output : doc list
 * 1. Build Scorer by Query, involve Weight(read/iterator
 *    inverted table data from db).
 * 2. Build Collector to collect all doc id.
 * 3. Filter doc id list.
 * 4. Sort doc id list.
 * 5. Ouput.
 */
class Searcher {
 private:
  IndexConfig *config_;

 public:
  Searcher(IndexConfig *config) : config_(config) {}

  virtual ~Searcher() {}

  // If min_match_filter_num = 0,all filter must be match.
  SearchStatus DoQuery(
      const TableID &table, Query &query, size_t offset, size_t limit,
      std::vector<Filter *> *filter, std::vector<SortCondition *> *sorter,
      std::list<DocumentID> &docs,
      std::vector<std::shared_ptr<ScoreStrategy>> *score_strategy_list =
          nullptr,
      size_t max_score_doc_num = SIZE_MAX, uint32_t min_match_filter_num = 0,
      wwsearch::SearchTracer *tracer = nullptr,
      uint32_t *get_match_total_cnt = nullptr);

  // GetDocument
  // If success,status is ok,and document will fill with content.
  SearchStatus GetStoredFields(const TableID &table,
                               std::vector<Document *> &docs,
                               std::vector<SearchStatus> &status,
                               SearchContext *context);

  // GetDocValue
  SearchStatus GetDocValue(const TableID &table, std::vector<Document *> &docs,
                           std::vector<SearchStatus> &status,
                           SearchContext *context);
  // Support post filter?
  SearchStatus DoPostFilter() { return SearchStatus(); }

  // Support sort

  // Support aggeration
  SearchStatus DoAggregation() { return SearchStatus(); }

  // use for get all entity
  // only get kDB kPaxosDataMetaColumn column
  // key format must like businesstype+partition_set
  // WTF OF certain
  // if empty if start_partition_set = 0;
  SearchStatus ScanBusinessType(uint8_t business_type,
                                uint64_t &start_partition_set, size_t max_len,
                                std::vector<uint64_t> &sets,
                                VirtualDBSnapshot *snapshot = nullptr);

  // only used for certain recover!
  // Note: return status is ok and start_key is empty,data reach end.
  // max_len must > 1
  SearchStatus ScanTableData(const TableID &table,
                             wwsearch::StorageColumnType column,
                             std::string &start_key, size_t max_len,
                             std::string &write_batch,
                             VirtualDBSnapshot *snapshot = nullptr,
                             size_t max_buffer_size = 18 * 1024 *
                                                      1024 /*18MB*/);

 private:
  // 0->stored field
  // 1->doc value
  SearchStatus InnerGetFields(int mode, const TableID &table,
                              std::vector<Document *> &docs,
                              std::vector<SearchStatus> &status,
                              SearchContext *context);
};  // namespace wwsearch
}  // namespace wwsearch
