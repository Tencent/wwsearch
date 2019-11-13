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

#include "document.h"
#include "index_config.h"
#include "search_status.h"
#include "storage_type.h"
#include "tracer.h"

namespace wwsearch {

using TermFlagPairList = std::map<std::string, unsigned char>;
using TermFlagPairListPtr = std::unique_ptr<TermFlagPairList>;

class DocumentWriter {
 private:
  IndexConfig* config_;

 public:
  DocumentWriter();

  ~DocumentWriter();

  bool SetConfig(IndexConfig* config);

  // Update each document pair
  // Find the differences between old document and new document.Write document
  // to store field ,write inverted term index,write document value...
  SearchStatus UpdateDocuments(const TableID& table,
                               std::vector<DocumentUpdater*>& documents,
                               std::string* store_buffer,
                               SearchTracer* tracer = nullptr);

  SearchStatus DeleteDocs(const TableID& table,
                          std::vector<DocumentID>& document_ids,
                          SearchTracer* tracer = nullptr);

  SearchStatus IngestInvertIndex(const TableID& table,
                                 InvertIndexItemList& indices,
                                 std::string* store_buffer,
                                 SearchTracer* tracer = nullptr);
  typedef struct AllocSequence {
    std::string user_id;
    uint64_t sequence;
    SearchStatus status;
    AllocSequence() { sequence = 0; }
  } AllocSequence;

  SearchStatus AcquireCurrentSequence(const TableID& table,
                                      uint64_t& current_max,
                                      std::vector<AllocSequence>& sequence_list,
                                      SearchTracer* tracer = nullptr);

  SearchStatus AcquireNewSequence(const TableID& table,
                                  std::vector<AllocSequence>& sequence_list,
                                  std::string* store_buffer,
                                  SearchTracer* tracer = nullptr);

  SearchStatus DropTable(const TableID& table, std::string* store_buffer,
                         SearchTracer* tracer = nullptr);

 private:
  SearchStatus WriteStoredField(const TableID& table,
                                std::vector<DocumentUpdater*>& documents,
                                WriteBuffer& write_buffer,
                                SearchTracer* tracer = nullptr);

  SearchStatus WriteDocValue(const TableID& table,
                             std::vector<DocumentUpdater*>& documents,
                             WriteBuffer& write_buffer,
                             SearchTracer* tracer = nullptr);

  SearchStatus WriteInvertedIndex(const TableID& table,
                                  std::vector<DocumentUpdater*>& documents,
                                  WriteBuffer& write_buffer,
                                  SearchTracer* tracer = nullptr);

  SearchStatus WriteTableMeta(const TableID& table,
                              std::vector<DocumentUpdater*>& documents,
                              WriteBuffer& write_buffer,
                              SearchTracer* tracer = nullptr);

  SearchStatus WriteDictionaryMeta(const TableID& table,
                                   std::vector<DocumentUpdater*>& documents,
                                   WriteBuffer& write_buffer,
                                   SearchTracer* tracer = nullptr);

  SearchStatus RunTokenizer(std::vector<DocumentUpdater*>& documents,
                            SearchTracer* tracer = nullptr);

  SearchStatus MergeOldField(std::vector<DocumentUpdater*>& documents,
                             SearchTracer* tracer = nullptr);
  void MergeDocvalue(Document& old_docvalue_document,
                     Document* new_docvalue_document);

  SearchStatus WriteStoreFieldForDropTable(const TableID& table,
                                           WriteBuffer& write_buffer,
                                           SearchTracer* tracer = nullptr);

  SearchStatus WriteInvertedIndexForDropTable(const TableID& table,
                                              WriteBuffer& write_buffer,
                                              SearchTracer* tracer = nullptr);

  SearchStatus WriteDocvalueForDropTable(const TableID& table,
                                         WriteBuffer& write_buffer,
                                         SearchTracer* tracer = nullptr);

  SearchStatus WriteTableMetaForDropTable(const TableID& table,
                                          WriteBuffer& write_buffer,
                                          SearchTracer* tracer = nullptr);

  SearchStatus WriteDictionaryMetaForDropTable(const TableID& table,
                                               WriteBuffer& write_buffer,
                                               SearchTracer* tracer = nullptr);
};

}  // namespace wwsearch
