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

#include <iostream>

#include "include/codec_impl.h"
#include "include/document.h"
#include "include/index_writer.h"
#include "include/logger.h"
#include "include/virtual_db_rocks.h"

#include "include/and_query.h"
#include "include/bool_query.h"
#include "include/index_wrapper.h"
#include "include/query.h"
#include "include/searcher.h"
#include "include/storage_type.h"
#include "include/tokenizer_impl.h"
#include "include/weight.h"

// A simple example for index documents and search match terms.
int main(int argc, char **argv) {
  wwsearch::DefaultIndexWrapper indexer;
  bool use_rocksdb = false;
  if (argc == 1) {
    // use mock(simple memory) db
  } else if (argc == 2) {
    // use rocksdb
    indexer.DBParams().path.assign(argv[1]);
    use_rocksdb = true;
  } else {
    printf("Please check input args\n");
    return -1;
  }
  indexer.Config().SetLogLevel(wwsearch::kSearchLogLevelDebug);

  wwsearch::SearchStatus status = indexer.Open(use_rocksdb);
  SearchLogDebug("status:%s", status.GetState().c_str());
  assert(status.OK());

  // construct
  wwsearch::IndexFieldFlag flag;
  flag.SetStoredField();
  flag.SetTokenize();
  flag.SetSuffixBuild();
  flag.SetDocValue();
  flag.SetInvertIndex();

  std::vector<wwsearch::DocumentUpdater *> documents;
  for (int i = 0; i < 3; i++) {
    wwsearch::DocumentUpdater *document_updater =
        new wwsearch::DocumentUpdater();
    wwsearch::Document &document = document_updater->New();
    document.SetID(i + 1);  // document id must start from 1.

    // 0 -> word
    // 1 -> filter_value
    // 2 -> sort_value
    {
      auto field = document.AddField();
      field->SetMeta(0, flag);
      field->SetString("one two three");
    }
    {
      auto field = document.AddField();
      field->SetMeta(1, flag);
      field->SetUint32(100 + i);
    }
    {
      auto field = document.AddField();
      field->SetMeta(2, flag);
      field->SetUint32(1000 + i);
    }
    documents.push_back(document_updater);
    std::string debug_str;
    document.PrintToReadStr(debug_str);
    std::cout << "Insert Documents" << std::endl << debug_str << std::endl;
  }

  // add
  wwsearch::TableID table;
  table.business_type = 1;
  table.partition_set = 10000;
  bool success = indexer.index_writer_->AddDocuments(table, documents);
  if (!success) {
    std::cout << "Add Document return error" << std::endl;
  }

  // check
  std::cout << "After Add Status:" << std::endl;
  for (auto du : documents) {
    std::cout << "  document_id:" << du->New().ID();
    std::cout << " Code:" << (du->Status().OK() ? "Success" : "Failure")
              << " State:" << du->Status().GetState() << std::endl;
    delete du;
  }

  // do search
  const char *terms1[] = {"one", "two", "tw", "ne"};
  const char *terms2[] = {"two", "three", "tw", "ne"};

  for (size_t i = 0; i < sizeof(terms1) / sizeof(const char *); i++) {
    wwsearch::Searcher searcher(&indexer.Config());
    std::string term1(terms1[i]);
    std::string term2(terms2[i]);

    wwsearch::AndQuery query;

    std::list<wwsearch::DocumentID> match_docids;
    std::vector<wwsearch::Document *> match_documents;
    std::vector<wwsearch::SearchStatus> ss;

    wwsearch::BooleanQuery query1(0, term1);
    wwsearch::BooleanQuery query2(0, term2);
    query.AddQuery(&query1);
    query.AddQuery(&query2);

    std::vector<wwsearch::Filter *> filter;
    {
      wwsearch::RangeFilter *rule = new wwsearch::RangeFilter(102, 103);
      rule->GetField()->SetMeta(1, flag);
      rule->GetField()->SetUint32(0);
      filter.push_back(rule);
    }
    std::vector<wwsearch::SortCondition *> sorter;
    {
      wwsearch::NumericSortCondition *sort =
          new wwsearch::NumericSortCondition(2, wwsearch::kSortConditionDesc);
      sorter.push_back(sort);
    }

    auto status =
        searcher.DoQuery(table, query, 0, 10, &filter, &sorter, match_docids);
    std::cout << "query: match " << match_docids.size() << std::endl;
    std::cout << "	term=" << term1 << " and " << term2 << std::endl;
    for (auto rule : filter)
      std::cout << "	filter=" << rule->PrintReadableStr() << std::endl;
    std::cout << std::endl;
    if (status.OK()) {
      for (auto doc : match_docids) {
        wwsearch::Document *document = new wwsearch::Document;
        document->SetID(doc);
        match_documents.push_back(document);
      }

      // now we fetch document again.
      auto ret = searcher.GetStoredFields(table, match_documents, ss, nullptr);
      if (ret.OK()) {
        for (size_t i = 0; i < ss.size(); i++) {
          if (ss[i].OK()) {
            std::string debug_str;
            match_documents[i]->PrintToReadStr(debug_str);
            std::cout << debug_str << std::endl;
          } else {
            std::cout << "Error:Get Document " << match_documents[i]->ID()
                      << "," << ss[i].GetCode() << "," << ss[i].GetState()
                      << std::endl;
          }
        }
      } else {
        std::cout << "Error:GetStoredFields fail," << ret.GetCode() << ","
                  << ret.GetState() << std::endl;
      }
    } else {
      std::cout << "Error:fail,code:" << status.GetCode()
                << ",msg:" << status.GetState() << std::endl;
    }

    for (auto rule : filter) delete rule;
    for (auto sort : sorter) delete sort;
    for (auto d : match_documents) delete d;
  }

  return 0;
}
