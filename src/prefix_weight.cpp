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

#include "prefix_weight.h"
#include "func_scope_guard.h"
#include "header.h"
#include "or_iterator.h"
#include "prefix_scorer.h"
#include "search_status.h"
#include "storage_type.h"
#include "utils.h"

namespace wwsearch {

PrefixWeight::PrefixWeight(PrefixQuery *query)
    : Weight(query, "PrefixWeight"), or_iterator(nullptr), codec_(nullptr) {}

PrefixWeight::~PrefixWeight() {
  if (nullptr != or_iterator) {
    delete or_iterator;
    or_iterator = nullptr;
  }

  for (auto it : this->iterators) {
    assert(nullptr != codec_);
    codec_->ReleaseDocListReaderCodec(it);
  }
  this->iterators.clear();
}

// prefix query,to find limit prefix term
Scorer *PrefixWeight::GetScorer(SearchContext *context) {
  Codec *codec = context->GetConfig()->GetCodec();
  PrefixQuery *prefix_query = reinterpret_cast<PrefixQuery *>(this->GetQuery());
  auto db = context->VDB();
  {
    VirtualDBReadOption options;
    options.snapshot_ = context->GetSnapshot();
    auto iterator = db->NewIterator(kInvertedIndexColumn, &options);
    std::string prefix_key;
    codec->EncodeInvertedKey(context->Table(), prefix_query->GetFieldID(),
                             prefix_query->MatchTerm(), prefix_key);

    SearchLogDebug(
        "PrefixWeight::GetScorer prefix_key(tableID=%s, FieldID=%u, "
        "MatchTerm=%s), prefix key size=%d",
        context->Table().PrintToStr().c_str(), prefix_query->GetFieldID(),
        prefix_query->MatchTerm().c_str(), prefix_key.size());
    uint32_t total_doc_list_size = 0;
    for (iterator->Seek(prefix_key); iterator->Valid(); iterator->Next()) {
      // reach max limit
      if (total_doc_list_size >= prefix_query->MaxDocListSize()) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "PrefixWeight::GetScorer default MaxDocListSize(%lu), "
                 "total_doc_list_size(%lu)",
                 prefix_query->MaxDocListSize(), total_doc_list_size);
        SearchLogError("%s", buf);
        context->Status().SetStatus(kReachMaxDocListSizeLimit, buf);
        break;
      }

      // have same prefix ?
      if (iterator->key().size() < prefix_key.size() ||
          0 != memcmp(prefix_key.c_str(), iterator->key().data(),
                      prefix_key.size())) {
        SearchLogDebug(
            "PrefixWeight::GetScorer not same prefix, break "
            "prefix_key(tableID=%s, FieldID=%u, "
            "MatchTerm=%s) size=%d, "
            "iterator->key size=%d",
            context->Table().PrintToStr().c_str(), prefix_query->GetFieldID(),
            prefix_query->MatchTerm().c_str(), prefix_key.size(),
            iterator->key().size());
        for (int i = 0; i < prefix_key.size(); ++i) {
          SearchLogDebug("%d , %c , %c\n", i, prefix_key[i],
                         *(iterator->key().data() + i));
        }
        break;
      }
      total_doc_list_size += iterator->value().size();
      this->values_.emplace_back(iterator->value().data(),
                                 iterator->value().size());
    }
    delete iterator;
  }

  SearchLogDebug("match prefix term number=%d\n", this->values_.size());

  // Note: may return empty values_ because no one doc match.
  or_iterator = new OrIterator();
  for (auto &value : values_) {
    DocListReaderCodec *doc_lists = codec->NewDocListReaderCodec(
        value.c_str(), value.size(), prefix_query->GetFieldID());
    this->iterators.push_back(doc_lists);
    or_iterator->AddSubIterator(doc_lists);

    SearchLogDebug("doclist/value[%d %s]\n", value.size(),
                   DebugInvertedValueByReader(codec, value).c_str());
  }
  or_iterator->FinishAddIterator();
  codec_ = codec;
  PrefixScorer *scorer = new PrefixScorer(this, or_iterator);
  return scorer;
}

BulkScorer *PrefixWeight::GetBulkScorer(SearchContext *context) {
  return nullptr;
}

}  // namespace wwsearch
