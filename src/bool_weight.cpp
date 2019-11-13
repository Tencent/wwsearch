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

#include "bool_weight.h"
#include "bool_scorer.h"
#include "codec_doclist_impl.h"
#include "func_scope_guard.h"
#include "header.h"
#include "search_status.h"
#include "storage_type.h"
#include "utils.h"

namespace wwsearch {

BooleanWeight::BooleanWeight(BooleanQuery *query)
    : Weight(query, "BooleanWeight") {}

BooleanWeight::~BooleanWeight() {}

Scorer *BooleanWeight::GetScorer(SearchContext *context) {
  Codec *codec = context->GetConfig()->GetCodec();
  BooleanQuery *query = reinterpret_cast<BooleanQuery *>(this->GetQuery());

  std::vector<StorageColumnType> columns;
  std::vector<std::string> keys;
  std::vector<SearchStatus> status;
  {
    std::string key;
    if (query->ValueType() == kStringIndexField) {
      codec->EncodeInvertedKey(context->Table(), query->GetFieldID(),
                               query->MatchTerm(), key);
    } else if (query->ValueType() == kUint32IndexField) {
      std::string term;
      AppendFixed32(term, query->MatchNumeric());
      codec->EncodeInvertedKey(context->Table(), query->GetFieldID(), term,
                               key);
    } else if (query->ValueType() == kUint64IndexField) {
      std::string term;
      AppendFixed64(term, query->MatchNumeric());
      codec->EncodeInvertedKey(context->Table(), query->GetFieldID(), term,
                               key);
    } else {
      context->Status().SetStatus(kScorerErrorStatus, "unknow valuetype");
      return nullptr;
    }
    SearchLogDebug("BooleanWeight::GetScorer DebugInvertedKey key(%s)",
                   codec->DebugInvertedKey(key).c_str());
    keys.push_back(key);
    columns.push_back(kInvertedIndexColumn);
  }
  // TODO: need used snapshot for read
  context->VDB()->MultiGet(columns, keys, values_, status,
                           nullptr /*context->GetSnapshot()*/);
  assert(keys.size() == status.size());
  SearchLogDebug(
      "GetScorer Table(%s), FieldID(%u), match_term(%s), keys_size(%d), "
      "keys(%s), values_size(%d), values(%s)",
      context->Table().PrintToStr().c_str(), query->GetFieldID(),
      query->MatchTerm().c_str(), keys.size(),
      JoinContainerToString(keys, ";").c_str(), values_.size(),
      JoinContainerToString(values_, ";").c_str());
  for (auto ss : status) {
    if (!ss.OK()) {
      // document not exist is ok,just return empty scorer
      if (!ss.DocumentNotExist()) {
        context->Status() = ss;
        return nullptr;
      }
      // set to zero
      values_[0].clear();
    }
  }

  SearchLogDebug("doclist len:%llu ", values_[0].size());
  if (values_[0].size() > 0) {
    SearchLogDebug(
        "GetScorer Table(%s), FieldID(%u), match_term(%s) values_[0](%s)",
        context->Table().PrintToStr().c_str(), query->GetFieldID(),
        query->MatchTerm().c_str(),
        DebugInvertedValueByReader(codec, values_[0]).c_str());
  }
  assert(values_.size() == 1);
  DocListReaderCodec *doc_lists = codec->NewDocListReaderCodec(
      values_[0].c_str(), values_[0].size(), query->GetFieldID());

  // codec will release doc_lists.
  BooleanScorer *scorer = new BooleanScorer(this, doc_lists, codec);
  return scorer;
}

BulkScorer *BooleanWeight::GetBulkScorer(SearchContext *context) {
  return nullptr;
}

}  // namespace wwsearch
