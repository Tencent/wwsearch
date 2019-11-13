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

#include "collector_top.h"
#include "searcher.h"

namespace wwsearch {

// collect one document in heap,if head is full ,lowwer priority document will
// be drop.
void TopNCollector::Collect(DocumentID doc_id, int field_id) {
  SearchLogDebug("collect doc=%lu", doc_id);
  IndexConfig *index_config = search_context_->GetConfig();
  if (inner_purge_total_docs_count_ >
      index_config->GetMaxInnerPurgeDocsTotalLimit()) {
    if (nullptr != tracer_) {
      tracer_->Add(TracerType::kExceedInnerPurgeDocsTotalLimitCount, 1);
    }
    if (buffer_docs_.size() > 0) {
      InnerPurge();
      assert(buffer_docs_.empty());
    }
    return;
  }

  Document *document = new Document();
  document->SetID(doc_id);
  document->SetMatchFieldID(field_id);
  buffer_docs_.push_back(document);

  inner_purge_total_docs_count_ += 1;

  if (buffer_docs_.size() >= index_config->GetMaxInnerPurgeBatchDocsCount()) {
    InnerPurge();
    assert(buffer_docs_.empty());
  }
}

// If we can finish collect?
bool TopNCollector::Enough() {
  if (!status_.OK()) return true;
  if (!quick_top_n) return false;
  return (this->topN_docs_.size() >= this->top_n_) &&
         (nullptr ==
          get_match_total_cnt_) /* get_match_total_cnt nullptr means no need get
                                  total match doc count, no need iterator all */
      ;
}

// support score priority queue
void TopNCollector::HandleScorePriorityQueue() {
  if (use_score_strategy_ && score_priority_queue_.size() > 0) {
    assert(topN_docs_.size() == 0);
    while (score_priority_queue_.size() > 0) {
      if (topN_docs_.size() < limit_ && offset_ == 0) {
        SearchLogInfo(
            "score_priority_queue_ pop document id = %llu, match field "
            "id = %u, doc score = %f",
            score_priority_queue_.top()->ID(),
            score_priority_queue_.top()->MatchFieldId(),
            score_priority_queue_.top()->Score());
        topN_docs_.push(score_priority_queue_.top());
      } else if (offset_ > 0) {
        offset_--;
        SearchLogInfo(
            "drop by offset {%u} score_priority_queue_ pop document id = "
            "%llu, match field id = %u, doc score = %f",
            offset_, score_priority_queue_.top()->ID(),
            score_priority_queue_.top()->MatchFieldId(),
            score_priority_queue_.top()->Score());
        delete score_priority_queue_.top();
      } else {
        SearchLogInfo(
            "drop score_priority_queue_ pop document id = %llu, match "
            "field id "
            "= %u, doc score = %f",
            score_priority_queue_.top()->ID(),
            score_priority_queue_.top()->MatchFieldId(),
            score_priority_queue_.top()->Score());
        delete score_priority_queue_.top();
      }
      score_priority_queue_.pop();
    }
  }
}

// Finish our job.
void TopNCollector::Finish() {
  InnerPurge();
  HandleScorePriorityQueue();
}

// When we finish our job,collect all match documents id we needed.
void TopNCollector::GetAndClearMatchDocs(std::list<DocumentID> &docs) {
  while (topN_docs_.size() != 0 && top_n_ > 0) {
    docs.push_front(this->topN_docs_.top()->ID());
    delete this->topN_docs_.top();
    this->topN_docs_.pop();
    --top_n_;
  }

  while (offset_ > 0 && docs.size() > 0) {
    SearchLogDebug("Drop by offset {%u} docs list size {%u} document id = %llu",
                   offset_, docs.size(), docs.front());
    docs.remove(docs.front());
    --offset_;
  }
}

// Read document's info to check filter etc.
void TopNCollector::InnerPurge() {
  if (buffer_docs_.empty()) return;

  // Optimize
  if (nullptr == sorter_ && nullptr == filter_ && !use_score_strategy_) {
    for (size_t i = 0; i < buffer_docs_.size(); i++) {
      if (topN_docs_.size() < top_n_) {
        topN_docs_.push(buffer_docs_[i]);
        SearchLogDebug(
            "default push doc id {%llu} topN_docs {%d} document id = %llu",
            buffer_docs_[i]->ID(), topN_docs_.size(), topN_docs_.top()->ID());
        buffer_docs_[i] = nullptr;
      } else {
        delete buffer_docs_[i];
      }
    }

    if (nullptr != get_match_total_cnt_) {
      (*get_match_total_cnt_) += buffer_docs_.size();
    }

    buffer_docs_.clear();
    return;
  }

  std::vector<SearchStatus> ss;
  SearchLogDebug("buffer_docs_size:%u", buffer_docs_.size());
  {
    TimeCostCounter get_docvalue_consume_us;
    get_docvalue_consume_us.Start();
    auto ret =
        this->searcher_->GetDocValue(table_, buffer_docs_, ss, search_context_);
    if (!ret.OK()) {
      for (auto document : buffer_docs_) {
        delete document;
      }
      buffer_docs_.clear();
      status_ = ret;
      return;
    }
    tracer_->Add(TracerType::kGetDocvalueConsumeUsInCollector,
                 get_docvalue_consume_us.CostUs());
    tracer_->Add(TracerType::kGetDocvalueCountInCollector, ss.size());
  }

  std::vector<Document *> hit_documents;

  {
    TimeCostCounter filter_consume_us;
    filter_consume_us.Start();
    assert(ss.size() == buffer_docs_.size());
    uint64_t filter_hit_count = 0;
    for (size_t i = 0; i < ss.size(); i++) {
      if (ss[i].OK() || ss[i].DocumentNotExist()) {
        auto document = buffer_docs_[i];
        if (!MatchFilter(document)) {
          SearchLogDebug("filter not pass doc=%lu\n", document->ID());
          continue;
        }
        filter_hit_count++;

        if (nullptr != get_match_total_cnt_) {
          (*get_match_total_cnt_)++;
        }

        SearchLogDebug("filter hit collect doc=%lu\n", document->ID());
        hit_documents.push_back(document);

        /*
        bool need_purge = topN_docs_.size() > top_n_;
        if (need_purge) {
          delete topN_docs_.top();
          topN_docs_.pop();
        }
        */
        buffer_docs_[i] = nullptr;

      } else {
        // meet other error
        status_ = ss[i];
        break;
      }
    }
    tracer_->Add(TracerType::kFilterConsumeUsInCollector,
                 filter_consume_us.CostUs());
    tracer_->Add(TracerType::kFilterHitCountInCollector, filter_hit_count);
  }

  SearchLogDebug("Before GetStoredFields");

  if (hit_documents.size() > 0) {
    if (use_score_strategy_) {
      if (score_doc_count_ < max_score_doc_num_) {
        SearchLogDebug("use_score_strategy enter!!!");
        score_doc_count_ += hit_documents.size();
        ss.clear();
        {
          TimeCostCounter get_storedoc_consume_us;
          get_storedoc_consume_us.Start();

          for (auto doc : hit_documents) {
            doc->ClearField();
          }

          auto ret = this->searcher_->GetStoredFields(table_, hit_documents, ss,
                                                      search_context_);
          if (!ret.OK()) {
            for (auto document : hit_documents) {
              delete document;
            }
            hit_documents.clear();
            status_ = ret;
            return;
          }
          tracer_->Add(TracerType::kGetStoredocConsumeUsInCollector,
                       get_storedoc_consume_us.CostUs());
          tracer_->Add(TracerType::kGetStoredocCountInCollector, ss.size());
        }

        TimeCostCounter post_scorer_consume_us;
        post_scorer_consume_us.Start();
        assert(ss.size() == hit_documents.size());
        for (size_t i = 0; i < ss.size(); i++) {
          if (ss[i].OK() || ss[i].DocumentNotExist()) {
            auto document = hit_documents[i];

            for (size_t j = 0; j < score_strategy_list_->size(); ++j) {
              document->AddScore((*score_strategy_list_)[j]->Score(
                  document, document->MatchFieldId()));
            }
            SearchLogInfo(
                "score_priority_queue_ push document id = %llu, match field id "
                "= %u, doc score = %f",
                document->ID(), document->MatchFieldId(), document->Score());
            score_priority_queue_.push(document);
            hit_documents[i] = nullptr;
          }
        }
        tracer_->Add(TracerType::kPostScorerConsumeUsInCollector,
                     post_scorer_consume_us.CostUs());
        tracer_->Add(TracerType::kPostScorerDocCountInCollector, ss.size());
      } else {
        HandleScorePriorityQueue();

        for (size_t i = 0; i < hit_documents.size(); ++i) {
          if (hit_documents[i] != nullptr) {
            delete hit_documents[i];
            hit_documents[i] = nullptr;
          }
        }
      }
    } else {
      for (size_t i = 0; i < hit_documents.size(); ++i) {
        topN_docs_.push(hit_documents[i]);
        SearchLogDebug(
            "not use_score_strategy topN_docs {%d} push document id = %llu, "
            "match "
            "field id = %u",
            topN_docs_.size(), topN_docs_.top()->ID(),
            topN_docs_.top()->MatchFieldId());
        if (topN_docs_.size() > top_n_) {
          SearchLogDebug(
              "not use_score_strategy topN_docs {%d} pop document id = %llu, "
              "match "
              "field id = %u",
              topN_docs_.size(), topN_docs_.top()->ID(),
              topN_docs_.top()->MatchFieldId());
          delete topN_docs_.top();
          topN_docs_.pop();
        }
        hit_documents[i] = nullptr;
      }
    }
  }

  for (auto document : buffer_docs_) {
    if (nullptr != document) delete document;
  }
  buffer_docs_.clear();
  return;
}

// Check if documents match filter.
bool TopNCollector::MatchFilter(Document *document) {
  if (nullptr == this->filter_) return true;
  uint32_t match_count = 0;
  for (auto rule : *this->filter_) {
    auto find_field = document->FindField(rule->GetFieldID());
    if (nullptr == find_field) {
      SearchLogDebug("document do not have field %llu", rule->GetFieldID());
    } else {
      std::string str;
      find_field->PrintToString(str);
      SearchLogDebug("field:%s", str.c_str());
    }
    if (!rule->Match(find_field)) {
      SearchLogDebug("not match");
      // return false;
    } else {
      match_count++;
    }
    SearchLogDebug("match");
  }
  SearchLogDebug("min_match:%u,real_match:%u", min_match_filter_num_,
                 match_count);
  if (match_count >= min_match_filter_num_) return true;
  return false;
}

}  // namespace wwsearch
