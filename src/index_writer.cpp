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

#include "index_writer.h"
#include "logger.h"

namespace wwsearch {

IndexWriter::IndexWriter() : config_(nullptr) {}

IndexWriter::~IndexWriter() {}

bool IndexWriter::Open(IndexConfig *config) {
  if (nullptr == config || this->config_ != nullptr) return false;
  this->config_ = config;
  this->document_writer_.SetConfig(this->config_);
  return true;
}

const IndexConfig &IndexWriter::Config() { return *(this->config_); }

// Add Documents If document not exist before.
// If store_buffer not null,encoded bytes will be assigned to it.
bool IndexWriter::AddDocuments(const TableID &table,
                               std::vector<DocumentUpdater *> &documents,
                               std::string *store_buffer,
                               SearchTracer *tracer) {
  return InnerWriteDocuments(table, documents, store_buffer, kDocumentAddType,
                             tracer);
}

// Update Documents if document exist before.
// Basic Test,Not Need Comment.
bool IndexWriter::UpdateDocuments(const TableID &table,
                                  std::vector<DocumentUpdater *> &documents,
                                  std::string *store_buffer,
                                  SearchTracer *tracer) {
  return InnerWriteDocuments(table, documents, store_buffer,
                             kDocumentUpdateType, tracer);
}

// Add Or Update Documents
// Basic Test,Not Need Comment.
bool IndexWriter::AddOrUpdateDocuments(
    const TableID &table, std::vector<DocumentUpdater *> &documents,
    std::string *store_buffer, SearchTracer *tracer) {
  return InnerWriteDocuments(table, documents, store_buffer,
                             kDocumentAddOrUpdateType, tracer);
}

// Replace the whole document If documents exist before.
bool IndexWriter::ReplaceDocuments(const TableID &table,
                                   std::vector<DocumentUpdater *> &documents,
                                   std::string *store_buffer,
                                   SearchTracer *tracer) {
  return InnerWriteDocuments(table, documents, store_buffer,
                             kDocumentReplaceType, tracer);
}

// Delete the whold document.
bool IndexWriter::DeleteDocuments(const TableID &table,
                                  std::vector<DocumentUpdater *> &documents,
                                  std::string *store_buffer,
                                  SearchTracer *tracer) {
  return InnerWriteDocuments(table, documents, store_buffer,
                             kDocumentDeleteType, tracer);
}

// WARNING::
// Optimize api for write.
// AddDocument  without check or update.If use this api,we do not support Update
// Or Delete in fulture for this document.
bool IndexWriter::AddDocumentsWithoutRead(
    const TableID &table, std::vector<DocumentUpdater *> &documents,
    std::string *store_buffer, SearchTracer *tracer) {
  return InnerWriteDocuments(table, documents, store_buffer,
                             kDocumentAddWithoutReadType, tracer);
}

static void UpdateDocumetStatus(
    std::vector<DocumentUpdater *> &documents, bool &ok,
    kDocumentUpdaterType mode, std::vector<std::string> document_values,
    std::vector<std::string> document_doc_values,
    std::vector<SearchStatus> multi_status,
    std::vector<SearchStatus> multi_docvalue_status) {
  for (size_t i = 0; i < multi_status.size(); i++) {
    SearchLogDebug(
        "InnerWriteDocuments before excute, db MultiGet doc_id{%llu}, code "
        "{%d}, state {%s}",
        documents[i]->New().ID(), multi_status[i].GetCode(),
        multi_status[i].GetState().c_str());

    if (kDocumentAddType == mode) {
      // add
      if (!multi_status[i].DocumentNotExist()) {
        ok = false;
        if (multi_status[i].OK()) {
          documents[i]->Status().SetStatus(kDocumentExistStatus,
                                           "Document exist before add");
        } else {
          documents[i]->Status() = multi_status[i];
        }
      }
    } else if (kDocumentUpdateType == mode) {
      // update
      if (!multi_status[i].OK()) {
        ok = false;
        documents[i]->Status() = multi_status[i];
        continue;
      }

      if (!documents[i]->Old().DeSerializeFromByte(document_values[i].c_str(),
                                                   document_values[i].size())) {
        ok = false;
        documents[i]->Status().SetStatus(kSerializeErrorStatus,
                                         "decode document error");
      }

      if (ok && multi_docvalue_status[i].DocumentExist()) {
        if (!documents[i]->OldDocValue().DeSerializeFromByte(
                document_doc_values[i].c_str(),
                document_doc_values[i].size())) {
          ok = false;
          documents[i]->Status().SetStatus(kSerializeErrorStatus,
                                           "decode document error");
        }
      }

    } else if (kDocumentAddOrUpdateType == mode) {
      // add or update
      if (multi_status[i].GetCode() == kDocumentNotExistStatus) {
        // not eixsit is ok status.
        continue;
      }

      if (!multi_status[i].OK()) {
        // other error happen
        ok = false;
        documents[i]->Status() = multi_status[i];
        continue;
      }

      if (!documents[i]->Old().DeSerializeFromByte(document_values[i].c_str(),
                                                   document_values[i].size())) {
        // parse error
        ok = false;
        documents[i]->Status().SetStatus(kSerializeErrorStatus,
                                         "decode old document error");
      }

      if (ok && multi_docvalue_status[i].DocumentExist()) {
        if (!documents[i]->OldDocValue().DeSerializeFromByte(
                document_doc_values[i].c_str(),
                document_doc_values[i].size())) {
          ok = false;
          documents[i]->Status().SetStatus(kSerializeErrorStatus,
                                           "decode document error");
        }
      }
    } else if (kDocumentDeleteType == mode) {
      // delete
      if (multi_status[i].GetCode() == kDocumentNotExistStatus) {
        // not exist,continue ?
        documents[i]->Status() = multi_status[i];
        continue;
      }

      if (!multi_status[i].OK()) {
        // other error
        documents[i]->Status() = multi_status[i];
        ok = false;
        continue;
      }

      if (!documents[i]->Old().DeSerializeFromByte(document_values[i].c_str(),
                                                   document_values[i].size())) {
        // parse error
        ok = false;
        documents[i]->Status().SetStatus(kSerializeErrorStatus,
                                         "decode old document error");
      }
    } else if (kDocumentReplaceType == mode) {
      // replace
      if (!multi_status[i].OK() &&
          multi_status[i].GetCode() != kDocumentNotExistStatus) {
        // other error happen.
        ok = false;
        documents[i]->Status() = multi_status[i];
        continue;
      }

      // if exist ,parse it
      if (multi_status[i].OK() &&
          !documents[i]->Old().DeSerializeFromByte(document_values[i].c_str(),
                                                   document_values[i].size())) {
        // parse error
        ok = false;
        documents[i]->Status().SetStatus(kSerializeErrorStatus,
                                         "decode old document error");
      }
    } else if (kDocumentAddWithoutReadType == mode) {
      // do nothing.
    } else {
      ok = false;
      SearchLogError("unknow logic,check mode:%d", mode);
    }
  }
}
// inner api
bool IndexWriter::InnerWriteDocuments(const TableID &table,
                                      std::vector<DocumentUpdater *> &documents,
                                      std::string *store_buffer,
                                      kDocumentUpdaterType mode,
                                      SearchTracer *tracer) {
  // must have one tracer
  SearchTracer tmp_tracer;
  if (nullptr == tracer) {
    tracer = &tmp_tracer;
  }
  bool ok = true;
  VirtualDB *vdb = this->config_->VDB();
  Codec *codec = this->config_->GetCodec();

  std::vector<std::string> document_keys;
  for (auto du : documents) {
    du->SetUpdateType(mode);
    Document &new_document = du->New();
    if (new_document.ID() == 0) {
      du->Status().SetStatus(kDocumentIDCanNotZero,
                             "document id can not be zero.");
      ok = false;
    }
    std::string document_key;
    codec->EncodeStoredFieldKey(table, new_document.ID(), document_key);
    SearchLogDebug("doc_id(%llu), StoredFieldKey(%s)", new_document.ID(),
                   codec->DebugStoredFieldKey(document_key).c_str());
    document_keys.push_back(document_key);
  }

  std::vector<std::string> document_values;
  std::vector<std::string> document_doc_values;
  std::vector<SearchStatus> multi_status;
  std::vector<SearchStatus> multi_docvalue_status;
  // If do not need read from disk?
  if (mode != kDocumentAddWithoutReadType) {
    timeval begin, end;
    gettimeofday(&begin, NULL);

    vdb->MultiGet(std::vector<StorageColumnType>(document_keys.size(),
                                                 kStoredFieldColumn),
                  document_keys, document_values, multi_status, nullptr);

    if (kDocumentUpdateType == mode || kDocumentAddOrUpdateType == mode) {
      SearchLogDebug(
          "MultiGet for Update/AddOrUpdate mode to get DocValueColumn for "
          "merge");
      vdb->MultiGet(
          std::vector<StorageColumnType>(document_keys.size(), kDocValueColumn),
          document_keys, document_doc_values, multi_docvalue_status, nullptr);
      assert(multi_docvalue_status.size() == documents.size());
    }

    gettimeofday(&end, NULL);
    tracer->Set(TracerType::kIndexWriterGetCount, 1);
    tracer->Set(TracerType::kIndexWriterGetKvCount, document_keys.size());

    uint64_t total_size = 0;
    for (const auto &key : document_keys) {
      total_size += key.size();
    }
    for (const auto &val : document_values) {
      total_size += val.size();
    }
    for (const auto &val : document_doc_values) {
      total_size += val.size();
    }
    tracer->Set(TracerType::kIndexWriterGetKvSizeBytes, total_size);
    tracer->Set(TracerType::kIndexWriterGetConsumeTimeUs,
                wwsearch::Time::CalculateConsumeTimeUs(begin, end));
    assert(multi_status.size() == documents.size());
  }

  UpdateDocumetStatus(documents, ok, mode, document_values, document_doc_values,
                      multi_status, multi_docvalue_status);

  if (ok) {
    auto status = this->document_writer_.UpdateDocuments(table, documents,
                                                         store_buffer, tracer);
    if (!status.OK()) {
      ok = false;
    }
  }

  if (!ok) {
    for (auto du : documents) {
      if (du->Status().OK()) {
        du->Status().SetStatus(
            kOtherDocumentErrorStatus,
            "Operation not finish,Some other document meet error in batch "
            "documents,Please check other document");
      }
    }
  }
  return ok;
}

// Delete the whole table
SearchStatus IndexWriter::DeleteTableData(TableID &table,
                                          wwsearch::StorageColumnType column,
                                          std::string &start_key,
                                          size_t max_len) {
  SearchStatus status;
  if (max_len <= 1) {
    status.SetStatus(kRocksDBErrorStatus, "max_len must > 1");
    return status;
  }
  std::string prefix;
  config_->GetCodec()->EncodeMetaKey(table, prefix);
  auto db = this->config_->VDB();
  VirtualDBReadOption options;
  Iterator *iterator = db->NewIterator(column, &options);
  if (nullptr == iterator) {
    status.SetStatus(kRocksDBErrorStatus, "new iterator fail");
    SearchLogError("kRocksDBErrorStatus new iterator fail");
    return status;
  }

  SearchLogDebug("");
  WriteBuffer *write_buffer = db->NewWriteBuffer(nullptr);
  std::string last_key;
  if (start_key.empty()) {
    iterator->Seek(prefix);
  } else {
    iterator->Seek(start_key);
  }
  for (; iterator->Valid() && max_len > 0; iterator->Next(), max_len--) {
    if (iterator->key().size() < prefix.size()) {
      SearchLogError("key size < prefix size %u", iterator->key().size());
      status.SetStatus(kRocksDBErrorStatus, "Key size too small");
      break;
    }
    assert(iterator->key().size() >= prefix.size());
    SearchLogDebug("iter key[%d, %s] prefix[%d, %s]", iterator->key().size(),
                   iterator->key().data(), prefix.size(), prefix.c_str());
    if (memcmp(iterator->key().data(), prefix.c_str(), prefix.size())) {
      break;
    }

    last_key.assign(iterator->key().data(), iterator->key().size());
    status = write_buffer->Delete(column, last_key);
    if (!status.OK()) {
      break;
    }
  }
  delete iterator;
  if (status.OK()) {
    status = db->FlushBuffer(write_buffer);
  }
  db->ReleaseWriteBuffer(write_buffer);
  if (status.OK()) {
    start_key = last_key;
  }
  return status;
}

// Inggest outer inverted index.
bool IndexWriter::IngestInvertIndex(const TableID &table,
                                    InvertIndexItemList &indices,
                                    std::string *store_buffer,
                                    SearchTracer *tracer) {
  SearchTracer tmp_tracer;
  if (nullptr == tracer) {
    tracer = &tmp_tracer;
  }
  SearchStatus status =
      document_writer_.IngestInvertIndex(table, indices, store_buffer, tracer);
  return status.OK();
}

// Read current sequence mapping.
bool IndexWriter::AcquireCurrentSequence(
    const TableID &table, uint64_t &current_max,
    std::vector<DocumentWriter::AllocSequence> &sequence_list,
    SearchTracer *tracer) {
  SearchTracer tmp_tracer;
  if (nullptr == tracer) {
    tracer = &tmp_tracer;
  }
  SearchStatus status = document_writer_.AcquireCurrentSequence(
      table, current_max, sequence_list, tracer);
  return status.OK();
}

// Read or Acquire sequence for userid.
bool IndexWriter::AcquireNewSequence(
    const TableID &table,
    std::vector<DocumentWriter::AllocSequence> &sequence_list,
    std::string *store_buffer, SearchTracer *tracer) {
  SearchTracer tmp_tracer;
  if (nullptr == tracer) {
    tracer = &tmp_tracer;
  }
  SearchStatus status = document_writer_.AcquireNewSequence(
      table, sequence_list, store_buffer, tracer);
  return status.OK();
}

// Drop table
bool IndexWriter::DropTable(const TableID &table, std::string *store_buffer,
                            SearchTracer *tracer) {
  SearchTracer tmp_tracer;
  if (nullptr == tracer) {
    tracer = &tmp_tracer;
  }
  SearchStatus status = document_writer_.DropTable(table, store_buffer, tracer);
  return status.OK();
}

}  // namespace wwsearch
