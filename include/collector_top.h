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

#include "collector.h"
#include "filter.h"
#include "post_scorer.h"
#include "sorter.h"
#include "tracer.h"

namespace wwsearch {
class Searcher;
class SearchContext;

using PriorityQueue =
    std::priority_queue<Document *, std::vector<Document *>, Sorter>;

using ScorePriorityQueue =
    std::priority_queue<Document *, std::vector<Document *>, PostScorer>;

/* Notice : Core class for collect & filter & sort the suitable doc list.
 * Main method is `InnerPurge()`. There are three periods:
 * 1. Collect :  Get doc list from Scorer, all doc id will be passed to
 *    TopNCollector
 *    by `void Collect(DocumentID doc, int field_id)`.
 *
 * 2. Filter : Base class `Filter` has a method `bool Match(const IndexField
 *    *field)`,
 *    iterator all vecter<Filter*> to filter all match doc list.
 *    Filter is based on DocValue column family which store in db.
 *
 * 3. PostScorer : Score the doc list by ScoreStrategy's list, including
 *    Complete keywords constructed from query request.
 *
 * 4. Sort : use PriorityQueue to sort the doc list.
 *    PriorityQueue with a Sorter compare function, support compare base on
 *    numeric.
 */
class TopNCollector : public Collector {
 private:
  PriorityQueue topN_docs_;
  ScorePriorityQueue score_priority_queue_;
  std::vector<SortCondition *> *sorter_;
  std::vector<std::shared_ptr<ScoreStrategy>> *score_strategy_list_;
  size_t max_score_doc_num_;
  size_t score_doc_count_;
  bool use_score_strategy_;
  std::vector<Document *> buffer_docs_;

  TableID table_;
  size_t top_n_;
  size_t offset_;
  size_t limit_;
  Searcher *searcher_;
  SearchContext *search_context_;

  std::vector<Filter *> *filter_;
  uint32_t inner_purge_total_docs_count_;
  uint32_t min_match_filter_num_;
  bool quick_top_n;
  wwsearch::SearchTracer *tracer_;
  uint32_t *get_match_total_cnt_;

 public:
  TopNCollector(
      TableID table, size_t offset, size_t limit, Searcher *searcher,
      SearchContext *search_context, std::vector<Filter *> *filter = nullptr,
      std::vector<SortCondition *> *sorter = nullptr,
      std::vector<std::shared_ptr<ScoreStrategy>> *score_strategy_list =
          nullptr,
      size_t max_score_doc_num = SIZE_MAX, uint32_t min_match_filter_num = 0,
      wwsearch::SearchTracer *tracer = nullptr,
      uint32_t *get_match_total_cnt = nullptr)
      : topN_docs_(Sorter(sorter)),
        score_priority_queue_(PostScorer()),
        sorter_(sorter),
        score_strategy_list_(score_strategy_list),
        max_score_doc_num_(std::max(offset + limit, max_score_doc_num)),
        score_doc_count_(0),
        use_score_strategy_(score_strategy_list != nullptr &&
                            score_strategy_list->size() > 0),
        table_(table),
        top_n_(offset + limit),
        offset_(offset),
        limit_(limit),
        searcher_(searcher),
        search_context_(search_context),
        filter_(filter),
        inner_purge_total_docs_count_(0),
        min_match_filter_num_(min_match_filter_num),
        quick_top_n(false),
        tracer_(tracer),
        get_match_total_cnt_(get_match_total_cnt) {
    if (nullptr != filter) {
      if (0 == min_match_filter_num_ ||
          min_match_filter_num_ > filter->size()) {
        min_match_filter_num_ = filter->size();
      }
    }
    quick_top_n = (nullptr == sorter);
    if (use_score_strategy_) {
      assert(max_score_doc_num_ >= top_n_);
    }
  }

  virtual ~TopNCollector() {
    for (auto document : buffer_docs_) {
      if (nullptr != document) delete document;
    }
    buffer_docs_.clear();
    while (score_priority_queue_.size() > 0) {
      delete score_priority_queue_.top();
      score_priority_queue_.pop();
    }
    while (topN_docs_.size() > 0) {
      delete topN_docs_.top();
      topN_docs_.pop();
    }
  }

  virtual void Collect(DocumentID doc, int field_id) override;

  virtual bool Enough() override;

  virtual void Finish() override;

  virtual void GetAndClearMatchDocs(std::list<DocumentID> &docs) override;

 private:
  inline void InnerPurge();

  bool MatchFilter(Document *document);

  void HandleScorePriorityQueue();
};

}  // namespace wwsearch
