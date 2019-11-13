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
#include "document_writer.h"
#include "index_config.h"
#include "index_writer.h"
#include "storage_type.h"
#include "tracer.h"

namespace wwsearch {

/* Notice : Index document user interface.
 * Input : std::vector<DocumentUpdater *>
 *      (DocumentUpdater includes New document which user wants to put.)
 * Output : success or not
 * 1. Check status of DocumentUpdater new document 's id from db.
 * 2. Merge with old document if need and exist.
 * 3. Build forward table kv.
 * 4. Build inverted table kv, attention this procedure need word segmentation.
 * 5. Build docvalue table kv.
 * 6. Flush to db.
 * 3-6 kv format refers to `codec_impl.h`.
 */
class IndexWriter {
 private:
  IndexConfig *config_;
  DocumentWriter document_writer_;

 public:
  IndexWriter();

  ~IndexWriter();

  bool Open(IndexConfig *config);

  // Return config instance. Need set config before IndexWriter is useable.
  const IndexConfig &Config();

  // Add document while the old document is not exist in database.
  // DO NOT have same DocumentID in documents.Each DocumentUpdater have its own
  // status code return. NOTE,if store_buffer not null,record will flush to this
  // buffer but not flush to db.
  bool AddDocuments(const TableID &table,
                    std::vector<DocumentUpdater *> &documents,
                    std::string *store_buffer = nullptr,
                    SearchTracer *tracer = nullptr);

  // Update document while the old document is exist in database.
  bool UpdateDocuments(const TableID &table,
                       std::vector<DocumentUpdater *> &documents,
                       std::string *store_buffer = nullptr,
                       SearchTracer *tracer = nullptr);

  // Update document no matter whether the old document exist in database or
  // not.
  bool AddOrUpdateDocuments(const TableID &table,
                            std::vector<DocumentUpdater *> &documents,
                            std::string *store_buffer = nullptr,
                            SearchTracer *tracer = nullptr);

  // replace with new documents nomatter whether the old document exist or not.
  bool ReplaceDocuments(const TableID &table,
                        std::vector<DocumentUpdater *> &documents,
                        std::string *store_buffer = nullptr,
                        SearchTracer *tracer = nullptr);

  // Just delete all match document in database.
  bool DeleteDocuments(const TableID &table,
                       std::vector<DocumentUpdater *> &documents,
                       std::string *store_buffer = nullptr,
                       SearchTracer *tracer = nullptr);

  // WARNING: Document will be insert into index without read from disk.Add
  // twice will make DocumentID duplicate in doc list.After merge,Only single
  // same DocumentID will be left.
  bool AddDocumentsWithoutRead(const TableID &table,
                               std::vector<DocumentUpdater *> &documents,
                               std::string *store_buffer = nullptr,
                               SearchTracer *tracer = nullptr);

  // Ingest InvertIndex only write inverted index but not write other data.
  bool IngestInvertIndex(const TableID &table, InvertIndexItemList &indices,
                         std::string *store_buffer,
                         SearchTracer *tracer = nullptr);

  // Read current max sequence from db.
  bool AcquireCurrentSequence(
      const TableID &table, uint64_t &current_max,
      std::vector<DocumentWriter::AllocSequence> &sequence_list,
      SearchTracer *tracer = nullptr);

  // Read current max sequence,If some not found,then alloc empty id to sequence
  // that start from current max.
  bool AcquireNewSequence(
      const TableID &table,
      std::vector<DocumentWriter::AllocSequence> &sequence_list,
      std::string *store_buffer, SearchTracer *tracer = nullptr);

  bool DropTable(const TableID &table, std::string *store_buffer,
                 SearchTracer *tracer = nullptr);

  // For certain
  // delete data
  SearchStatus DeleteTableData(TableID &table,
                               wwsearch::StorageColumnType column,
                               std::string &start_key, size_t max_len);

 private:
  // mode:
  // * 1->add
  // * 2->update
  // * 3->add or update
  bool InnerWriteDocuments(const TableID &table,
                           std::vector<DocumentUpdater *> &documents,
                           std::string *store_buffer, kDocumentUpdaterType mode,
                           SearchTracer *tracer = nullptr);
};

}  // namespace wwsearch
