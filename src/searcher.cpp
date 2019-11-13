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

#include "searcher.h"
#include "collector_top.h"

namespace wwsearch {

SearchStatus Searcher::DoQuery(
    const TableID &table, Query &query, size_t offset, size_t limit,
    std::vector<Filter *> *filter, std::vector<SortCondition *> *sorter,
    std::list<DocumentID> &docs,
    std::vector<std::shared_ptr<ScoreStrategy>> *score_strategy_list,
    size_t max_score_doc_num, uint32_t min_match_filter_num,
    wwsearch::SearchTracer *tracer, uint32_t *get_match_total_cnt) {
  wwsearch::SearchTracer internal_tracer;
  if (tracer == nullptr) {
    tracer = &internal_tracer;
  }
  SearchStatus status;
  VirtualDB *vdb = config_->VDB();
  VirtualDBSnapshot *snapshot = vdb->NewSnapshot();
  SearchContext context(table, vdb, snapshot, config_);

  Weight *weight = nullptr;
  Scorer *scorer = nullptr;

  {
    TimeCostCounter get_inverted_table_subquery_consume_us;
    get_inverted_table_subquery_consume_us.Start();

    weight = query.CreateWeight(&context, false, 0);
    scorer = weight->GetScorer(&context);

    tracer->Set(TracerType::kGetInvertedTableSubQueryConsumeUs,
                get_inverted_table_subquery_consume_us.CostUs());
  }

  if (nullptr == scorer) {
    status = context.Status();
    SearchLogDebug("DoQuery Couldn't get scorer for %s",
                   weight->Name().c_str());
    if (status.OK()) {
      status.SetStatus(kScorerErrorStatus, "can not get scorer");
    }
  } else {
    SearchLogDebug("score_strategy_list ? %d, max_score_doc_num %d ",
                   score_strategy_list != nullptr, max_score_doc_num);
    TopNCollector collector(table, offset, limit, this, &context, filter,
                            sorter, score_strategy_list, max_score_doc_num,
                            min_match_filter_num, tracer, get_match_total_cnt);
    collector.SetScorer(scorer);

    DocIdSetIterator &doc_lists = scorer->Iterator();
    while (doc_lists.DocID() != DocIdSetIterator::NO_MORE_DOCS &&
           !collector.Enough()) {
      SearchLogDebug("DoQuery Colloct DocID=%llu", doc_lists.DocID());
      collector.Collect(doc_lists.DocID(), doc_lists.FieldId());
      doc_lists.NextDoc();
    }
    collector.Finish();
    if (!collector.Status().OK()) {
      status = collector.Status();
    } else {
      collector.GetAndClearMatchDocs(docs);
      if (!context.Status().OK()) {
        status = context.Status();
      }
    }
  }
  delete weight;
  delete scorer;
  vdb->ReleaseSnapshot(snapshot);
  return status;
}

SearchStatus Searcher::GetStoredFields(const TableID &table,
                                       std::vector<Document *> &docs,
                                       std::vector<SearchStatus> &status,
                                       SearchContext *context) {
  return InnerGetFields(0, table, docs, status, context);
}

SearchStatus Searcher::GetDocValue(const TableID &table,
                                   std::vector<Document *> &docs,
                                   std::vector<SearchStatus> &status,
                                   SearchContext *context) {
  return InnerGetFields(1, table, docs, status, context);
}

SearchStatus Searcher::InnerGetFields(int mode, const TableID &table,
                                      std::vector<Document *> &docs,
                                      std::vector<SearchStatus> &status,
                                      SearchContext *context) {
  SearchStatus search_status;
  std::vector<StorageColumnType> columns;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  // build keys
  std::string document_key;
  for (size_t i = 0; i < docs.size(); i++) {
    this->config_->GetCodec()->EncodeStoredFieldKey(table, docs[i]->ID(),
                                                    document_key);
    if (0 == mode)
      columns.push_back(kStoredFieldColumn);
    else
      columns.push_back(kDocValueColumn);
    keys.push_back(std::move(document_key));
    assert(document_key.size() == 0);
  }

  // get from snapshot?
  if (nullptr != context) {
    context->VDB()->MultiGet(columns, keys, values, status,
                             context->GetSnapshot());
  } else {
    this->config_->VDB()->MultiGet(columns, keys, values, status, nullptr);
  }

  // parse
  assert(status.size() == docs.size());
  for (size_t i = 0; i < docs.size(); i++) {
    if (status[i].OK()) {
      if (!docs[i]->DeSerializeFromByte(values[i].c_str(), values[i].size())) {
        status[i].SetStatus(kSerializeErrorStatus, "Deserizlize bytes error");
      }
    }
  }
  return search_status;
}

SearchStatus Searcher::ScanBusinessType(uint8_t business_type,
                                        uint64_t &start_partition_set,
                                        size_t max_len,
                                        std::vector<uint64_t> &sets,
                                        VirtualDBSnapshot *snapshot) {
  SearchStatus status;
  if (max_len <= 1) {
    status.SetStatus(kRocksDBErrorStatus, "max_len must > 1");
    return status;
  }
  std::string prefix;
  AppendFixed8(prefix, business_type);
  auto db = this->config_->VDB();
  VirtualDBReadOption options;
  if (nullptr != snapshot) {
    options.snapshot_ = snapshot;
  }

  Iterator *iterator = db->NewIterator(kPaxosLogMetaColumn, &options);
  uint64_t last_parition_set = 0;
  if (nullptr != iterator) {
    std::string start_key = prefix;
    AppendFixed64(start_key, start_partition_set);
    for (iterator->Seek(start_key); iterator->Valid() && max_len > 0;
         iterator->Next()) {
      // meta len == 9, skip it ?
      if (iterator->key().size() < prefix.size()) {
        SearchLogError("error key len:%u", iterator->key().size());
        continue;
      }

      assert(iterator->key().size() >= prefix.size());
      if (memcmp(iterator->key().data(), prefix.c_str(), prefix.size())) {
        break;
      }

      auto key = iterator->key();
      uint8_t temp;
      RemoveFixed8(key, temp);
      RemoveFixed64(key, last_parition_set);
      sets.push_back(last_parition_set);
      max_len--;
    }
    delete iterator;
  } else {
    status.SetStatus(kRocksDBErrorStatus, "new iterator null");
  }
  if (status.OK()) {
    if (last_parition_set == start_partition_set) {
      // reach end ?
      start_partition_set = 0;
    } else {
      start_partition_set = last_parition_set;
    }
  }
  return status;
}

// only used for certain recover!
// Note: return status is ok and start_key is empty,data reach end.
// max_len must > 1
SearchStatus Searcher::ScanTableData(const TableID &table,
                                     wwsearch::StorageColumnType column,
                                     std::string &start_key, size_t max_len,
                                     std::string &write_batch,
                                     VirtualDBSnapshot *snapshot,
                                     size_t max_buffer_size) {
  SearchStatus status;
  if (max_len <= 1) {
    status.SetStatus(kRocksDBErrorStatus, "max_len must > 1");
    return status;
  }

  std::string prefix;
  config_->GetCodec()->EncodeMetaKey(table, prefix);
  SearchLogDebug("get prefix = Table[%s]", table.PrintToStr().c_str());
  VirtualDB *db = this->config_->VDB();
  WriteBuffer *write_buffer = db->NewWriteBuffer(nullptr);
  VirtualDBReadOption options;
  if (nullptr != snapshot) {
    options.snapshot_ = snapshot;
  }

  bool have_more_data = false;
  SearchLogDebug("");
  Iterator *iterator = db->NewIterator(column, &options);
  SearchLogDebug("");
  std::string last_key;
  if (nullptr != iterator) {
    if (start_key.empty()) {
      iterator->Seek(prefix);
    } else {
      iterator->Seek(start_key);
      // skip last key
      if (iterator->Valid()) {
        // Protect system :
        // User should use snapshot for read,so start_key must be same with
        // iterator's key Here add some protect for system
        Slice tmp_slice = iterator->key();
        if (tmp_slice.size() == start_key.size() &&
            0 ==
                memcmp(tmp_slice.data(), start_key.c_str(), tmp_slice.size())) {
          iterator->Next();
          SearchLogDebug("");
        } else {
          SearchLogError(
              "Please check request,start key not equal iterator's key,table "
              "%u:%lu",
              table.business_type, table.partition_set);
        }
      } else {
        // status.SetStatus(kDataErrorStatus, "start_key is invalid");
        SearchLogError(
            "Please check request,start_key is invalid,table "
            "%u:%lu",
            table.business_type, table.partition_set);
      }
    }
    for (; iterator->Valid() && max_len > 0; iterator->Next()) {
      // skip it ?
      if (iterator->key().size() < prefix.size()) {
        SearchLogError("too small key len:%u", iterator->key().size());
        continue;
      }

      assert(iterator->key().size() >= prefix.size());
      if (memcmp(iterator->key().data(), prefix.c_str(), prefix.size())) {
        break;
      }

      have_more_data = true;
      // if write batch buffer is too big,return error.
      // What will happen if one key's value is bigger than max_buffer_size?
      uint64_t approximate_size =
          iterator->key().size() + iterator->value().size();
      if (write_buffer->Data().size() + approximate_size > max_buffer_size) {
        if (approximate_size > max_buffer_size) {
          SearchLogError("error,size too big,table:%s,%lu",
                         table.PrintToStr().c_str(), approximate_size);
          std::string error_msg("keyvalue size too big,table:");
          error_msg.append(table.PrintToStr());
          status.SetStatus(kRocksDBErrorStatus, error_msg.c_str());
        }
        break;
      }

      last_key.assign(iterator->key().data(), iterator->key().size());
      std::string value;
      value.assign(iterator->value().data(), iterator->value().size());
      status = write_buffer->Put(column, last_key, value);
      if (!status.OK()) {
        SearchLogError("WriteBuffer put fail,%d:%s", status.GetCode(),
                       status.GetState().c_str());
        status.SetStatus(kRocksDBErrorStatus, "WriteBuffer put error");
        break;
      }
      max_len--;
    }
    write_batch = write_buffer->Data();
    delete iterator;
  } else {
    status.SetStatus(kRocksDBErrorStatus, "new iterator fail");
  }

  db->ReleaseWriteBuffer(write_buffer);
  if (status.OK()) {
    // If ok,set last key.
    // Protect system :
    // If we have more data,but do not return any data because of
    // buffer too big etc,return error.
    start_key = last_key;
    if (have_more_data && last_key.empty()) {
      status.SetStatus(kDataErrorStatus,
                       "We have more data,but last key is set to empty.");
    }
  }
  return status;
}

}  // namespace wwsearch
