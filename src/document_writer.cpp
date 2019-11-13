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

#include "document_writer.h"
#include <algorithm>
#include "codec_doclist_impl.h"
#include "logger.h"
#include "stat_collector.h"
#include "tokenizer.h"
#include "utf8_suffixbuilder.h"
#include "utils.h"

namespace wwsearch {

DocumentWriter::DocumentWriter() : config_(nullptr) {}

DocumentWriter::~DocumentWriter() {}

bool DocumentWriter::SetConfig(IndexConfig* config) {
  if (nullptr != this->config_) return false;
  this->config_ = config;
  return true;
}

// Update existed documents.
// If document not exist before,error will be returen to indicate the special
// case.
SearchStatus DocumentWriter::UpdateDocuments(
    const TableID& table, std::vector<DocumentUpdater*>& documents,
    std::string* store_buffer, SearchTracer* tracer) {
  VirtualDB* vdb = this->config_->VDB();
  // TODO:
  // optimiza,no copy ,if store_buffer not null
  WriteBuffer* write_buffer;
  if (nullptr != store_buffer && store_buffer->size() != 0) {
    write_buffer = vdb->NewWriteBuffer(store_buffer);
  } else {
    write_buffer = vdb->NewWriteBuffer(nullptr);
  }

  SearchStatus status;
  status = RunTokenizer(documents, tracer);
  // merge old document field to new document
  if (status.OK()) {
    status = MergeOldField(documents, tracer);
  }
  if (status.OK()) {
    status = WriteStoredField(table, documents, *write_buffer, tracer);
    if (nullptr != tracer) {
      tracer->Add(TracerType::kDocumentCount, documents.size());
    }
  }
  if (status.OK()) {
    status = WriteInvertedIndex(table, documents, *write_buffer, tracer);
  }
  if (status.OK()) {
    status = WriteDocValue(table, documents, *write_buffer, tracer);
  }
  if (status.OK()) {
    status = WriteTableMeta(table, documents, *write_buffer, tracer);
  }
  if (status.OK()) {
    status = WriteDictionaryMeta(table, documents, *write_buffer, tracer);
  }

  // check write batch size limit
  if (status.OK()) {
    if (write_buffer->DataSize() > this->config_->GetMaxWriteBatchSize()) {
      status.SetStatus(kDocumentTooLargeStatus,
                       "Document Too Large for write batch");
    }
  }

  if (status.OK()) {
    // only flush if store_buffer is null,else we will set
    if (nullptr == store_buffer) {
      timeval begin, end;
      gettimeofday(&begin, NULL);

      SearchLogDebug("");
      status = vdb->FlushBuffer(write_buffer);
      SearchLogDebug("");

      gettimeofday(&end, NULL);
      tracer->Set(TracerType::kDocumentWriterPutKvCount, 1);
      tracer->Set(TracerType::kDocumentWriterPutKvCount,
                  write_buffer->KvCount());
      tracer->Set(TracerType::kDocumentWriterPutKvSizeBytes,
                  write_buffer->DataSize());
      tracer->Set(TracerType::kDocumentWriterPutConsumeTimeUs,
                  Time::CalculateConsumeTimeUs(begin, end));

    } else {
      // TODO:optimize,use std::move ?
      *store_buffer = write_buffer->Data();
    }
    // reset status code.
    if (!status.OK()) {
      for (auto du : documents) {
        if (!du->Status().OK()) {
          du->Status() = status;
        }
      }
    }
  } else {
    for (auto du : documents) {
      if (du->Status().OK()) {
        du->Status().SetStatus(kOtherDocumentErrorStatus,
                               status.GetState().c_str());
      }
    }
  }
  vdb->ReleaseWriteBuffer(write_buffer);
  return status;
}

// Not support Delete documents now.
SearchStatus DocumentWriter::DeleteDocs(const TableID& table,
                                        std::vector<DocumentID>& document_ids,
                                        SearchTracer* tracer) {
  return SearchStatus();
}

// One special api to ingger outer inverted index.
SearchStatus DocumentWriter::IngestInvertIndex(const TableID& table,
                                               InvertIndexItemList& indices,
                                               std::string* store_buffer,
                                               SearchTracer* tracer) {
  SearchLogDebug("");
  SearchStatus status;
  VirtualDB* vdb = this->config_->VDB();
  WriteBuffer* write_buffer;
  if (nullptr != store_buffer && store_buffer->size() != 0) {
    write_buffer = vdb->NewWriteBuffer(store_buffer);
  } else {
    write_buffer = vdb->NewWriteBuffer(nullptr);
  }

  auto codec = this->config_->GetCodec();
  for (const InvertIndexItem* item : indices.List()) {
    std::string key;
    std::string value;
    codec->EncodeInvertedKey(table, item->GetFieldID(), item->GetTerm(), key);
    DocListWriterCodec* doc_list = codec->NewOrderDocListWriterCodec();
    assert(nullptr != doc_list);
    // in decrease order.
    for (auto doc_id : item->DocList()) {
      DocumentState s = kDocumentStateOK;
      doc_list->AddDocID(doc_id, s);
    }
    if (!doc_list->SerializeToBytes(value, 0)) {
      SearchLogError("Serialize error");
      status.SetStatus(kSerializeErrorStatus,
                       "Serialize to bytes error for invert doc list");
      break;
      assert(false);
    }

    codec->ReleaseOrderDocListWriterCodec(doc_list);

    status = write_buffer->Merge(kInvertedIndexColumn, key, value);
    if (!status.OK()) {
      SearchLogError("package buffer error") break;
    }
  }

  // If success ,package to store_buffer if need.
  if (status.OK()) {
    if (nullptr == store_buffer) {
      timeval begin, end;
      gettimeofday(&begin, NULL);

      status = vdb->FlushBuffer(write_buffer);

      gettimeofday(&end, NULL);
      tracer->Set(TracerType::kDocumentWriterPutKvCount, 1);
      tracer->Set(TracerType::kDocumentWriterPutKvCount,
                  write_buffer->KvCount());
      tracer->Set(TracerType::kDocumentWriterPutKvSizeBytes,
                  write_buffer->DataSize());
      tracer->Set(TracerType::kDocumentWriterPutConsumeTimeUs,
                  Time::CalculateConsumeTimeUs(begin, end));

    } else {
      *store_buffer = write_buffer->Data();
    }
  }

  vdb->ReleaseWriteBuffer(write_buffer);
  return status;
}

// Drop stored field of documents.
SearchStatus DocumentWriter::WriteStoreFieldForDropTable(
    const TableID& table, WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchStatus status;
  std::string begin_key;
  auto codec = this->config_->GetCodec();

  codec->EncodeStoredFieldKey(table, 0, begin_key);

  std::string end_key;
  codec->EncodeStoredFieldKey(table, UINT64_MAX, end_key);

  status = write_buffer.DeleteRange(kStoredFieldColumn, begin_key, end_key);
  return status;
}

// Drop inverted index of documents.
SearchStatus DocumentWriter::WriteInvertedIndexForDropTable(
    const TableID& table, WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchStatus status;
  std::string begin_key;
  auto codec = this->config_->GetCodec();
  codec->EncodeInvertedKey(table, 0, "", begin_key);

  std::string end_key;
  std::string max_term;
  for (int i = 0; i < 256; ++i) {
    max_term.append(1, char(255));
  }
  codec->EncodeInvertedKey(table, UINT8_MAX, max_term, end_key);
  status = write_buffer.DeleteRange(kInvertedIndexColumn, begin_key, end_key);
  return status;
}

// Drop doc-value of documents.
SearchStatus DocumentWriter::WriteDocvalueForDropTable(
    const TableID& table, WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchStatus status;
  std::string begin_key;
  auto codec = this->config_->GetCodec();
  codec->EncodeStoredFieldKey(table, 0, begin_key);

  std::string end_key;
  codec->EncodeStoredFieldKey(table, UINT64_MAX, end_key);

  status = write_buffer.DeleteRange(kDocValueColumn, begin_key, end_key);
  return status;
}

// Drop meta of documents.
SearchStatus DocumentWriter::WriteTableMetaForDropTable(
    const TableID& table, WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchStatus status;
  std::string meta_key;
  auto codec = this->config_->GetCodec();
  codec->EncodeMetaKey(table, meta_key);
  status = write_buffer.Delete(kMetaColumn, meta_key);
  return status;
}

// Drop dictionary of documents.
SearchStatus DocumentWriter::WriteDictionaryMetaForDropTable(
    const TableID& table, WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchStatus status;
  return status;
}

// Wrapper of Drop api
SearchStatus DocumentWriter::DropTable(const TableID& table,
                                       std::string* store_buffer,
                                       SearchTracer* tracer) {
  VirtualDB* vdb = this->config_->VDB();
  WriteBuffer* write_buffer;
  if (nullptr != store_buffer && store_buffer->size() != 0) {
    write_buffer = vdb->NewWriteBuffer(store_buffer);
  } else {
    write_buffer = vdb->NewWriteBuffer(nullptr);
  }

  SearchStatus status;
  status = WriteStoreFieldForDropTable(table, *write_buffer, tracer);

  if (status.OK()) {
    status = WriteInvertedIndexForDropTable(table, *write_buffer, tracer);
  }

  if (status.OK()) {
    status = WriteDocvalueForDropTable(table, *write_buffer, tracer);
  }

  if (status.OK()) {
    status = WriteTableMetaForDropTable(table, *write_buffer, tracer);
  }

  if (status.OK()) {
    status = WriteDictionaryMetaForDropTable(table, *write_buffer, tracer);
  }

  if (status.OK()) {
    if (nullptr == store_buffer) {
      status = vdb->FlushBuffer(write_buffer);
    } else {
      *store_buffer = write_buffer->Data();
    }
  }
  vdb->ReleaseWriteBuffer(write_buffer);
  return status;
}

// Read current sequence from db.
SearchStatus DocumentWriter::AcquireCurrentSequence(
    const TableID& table, uint64_t& current_max,
    std::vector<AllocSequence>& sequence_list, SearchTracer* tracer) {
  SearchLogDebug("");
  SearchStatus status;
  VirtualDB* vdb = this->config_->VDB();
  Codec* codec = this->config_->GetCodec();

  std::vector<StorageColumnType> columns;
  std::vector<std::string> ids;
  std::vector<std::string> values;
  // meta
  {
    std::string key;
    codec->EncodeSequenceMetaKey(table, key);
    ids.push_back(key);
    columns.push_back(kMetaColumn);
  }
  for (auto& item : sequence_list) {
    std::string key;
    codec->EncodeSequenceMappingKey(table, item.user_id, key);
    ids.push_back(key);
    columns.push_back(kMetaColumn);
  }

  assert(ids.size() == sequence_list.size() + 1);

  std::vector<SearchStatus> multi_status;
  {
    timeval begin, end;
    gettimeofday(&begin, NULL);

    vdb->MultiGet(columns, ids, values, multi_status, nullptr);

    gettimeofday(&end, NULL);
    tracer->Set(TracerType::kIndexWriterGetCount, 1);
    tracer->Set(TracerType::kIndexWriterGetKvCount, ids.size());

    assert(ids.size() == multi_status.size());
  }

  // check error
  for (size_t i = 0; i < multi_status.size(); i++) {
    if (!multi_status[i].OK() &&
        multi_status[i].GetCode() != kDocumentNotExistStatus) {
      status = multi_status[i];
      return status;
    }
  }

  // parse meta
  if (multi_status[0].GetCode() == kDocumentNotExistStatus) {
    current_max = 0;
  } else {
    if (values[0].size() != sizeof(uint64_t)) {
      status.SetStatus(kDataErrorStatus, "sequence's meta key size not match");
      return status;
    }
    current_max = *(uint64_t*)values[0].c_str();
  }

  // parse items
  for (size_t i = 1; i < multi_status.size(); i++) {
    AllocSequence& item = sequence_list[i - 1];
    if (multi_status[i].OK()) {
      if (values[i].size() != sizeof(uint64_t)) {
        status.SetStatus(kDataErrorStatus,
                         "sequence item's meta key size not match");
        return status;
      }
      item.sequence = *(uint64_t*)values[i].c_str();
    } else {
      item.status = multi_status[i];
    }
  }

  return status;
}  // namespace wwsearch

// Read sequence from db.
// Alloc new sequence if user_id have no mapping before.
SearchStatus DocumentWriter::AcquireNewSequence(
    const TableID& table, std::vector<AllocSequence>& sequence_list,
    std::string* store_buffer, SearchTracer* tracer) {
  SearchLogDebug("");
  SearchStatus status;

  uint64_t current_max;
  status = AcquireCurrentSequence(table, current_max, sequence_list, tracer);
  if (!status.OK()) {
    return status;
  }

  // DO NOT RETURN unless release writer buffer
  WriteBuffer* write_buffer;
  VirtualDB* vdb = this->config_->VDB();
  Codec* codec = this->config_->GetCodec();

  if (nullptr != store_buffer && store_buffer->size() != 0) {
    write_buffer = vdb->NewWriteBuffer(store_buffer);
  } else {
    write_buffer = vdb->NewWriteBuffer(nullptr);
  }

  uint64_t next_sequence = current_max;
  for (auto& item : sequence_list) {
    if (item.status.GetCode() == kDocumentNotExistStatus) {
      item.status.SetStatus(0, "");  // clear
      item.sequence = next_sequence + 1;
      next_sequence++;

      // encoded
      std::string key;
      std::string value;
      codec->EncodeSequenceMappingKey(table, item.user_id, key);
      value.assign((char*)&(item.sequence), sizeof(uint64_t));

      auto put_status = write_buffer->Put(kMetaColumn, key, value);
      if (!put_status.OK()) {
        status = put_status;
        break;
      }
    }
  }

  if (status.OK()) {
    if (next_sequence != current_max) {
      std::string key;
      std::string value;
      codec->EncodeSequenceMetaKey(table, key);
      value.assign((char*)(&next_sequence), sizeof(uint64_t));

      auto put_status = write_buffer->Put(kMetaColumn, key, value);
      if (!put_status.OK()) {
        status = put_status;
      } else {
        if (nullptr != store_buffer) {
          *store_buffer = write_buffer->Data();
        } else {
          status = vdb->FlushBuffer(write_buffer);
        }
      }
    } else {
      // If not update,clear store_buffer;
      if (nullptr != store_buffer) {
        store_buffer->clear();
      }
    }
  }

  vdb->ReleaseWriteBuffer(write_buffer);
  return status;
}

// Write stored field of documents.
SearchStatus DocumentWriter::WriteStoredField(
    const TableID& table, std::vector<DocumentUpdater*>& documents,
    WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchLogDebug("");

  Codec* codec = this->config_->GetCodec();
  SearchStatus status;
  for (auto du : documents) {
    if (du->Status().OK()) {
      Document& new_document = du->New();
      std::string document_key;
      std::string value;
      codec->EncodeStoredFieldKey(table, new_document.ID(), document_key);
      if (!du->Delete()) {
        if (new_document.SerializeToBytes(value, 0)) {
          du->Status() =
              write_buffer.Put(kStoredFieldColumn, document_key, value);
          lsmsearch::StoreDocument lsdoc;
          if (!lsdoc.ParseFromString(value)) {
            SearchLogError("lsdoc ParseFromString false!");
            assert(false);
          }

          SearchLogDebug(
              "WriteStoredField document_key:%s, document size:%u, doc : %s",
              codec->DebugStoredFieldKey(document_key).c_str(), value.size(),
              lsdoc.ShortDebugString().c_str());
        } else {
          du->Status().SetStatus(kSerializeErrorStatus,
                                 "Serialize kStoredFieldColumn error");
        }
      } else {
        du->Status() = write_buffer.Delete(kStoredFieldColumn, document_key);
      }
      if (!du->Status().OK()) {
        // some error happend during the operation of current document.
        status = du->Status();
        break;
      }
    }
  }

  return status;
}

// Merge new doc-value with document's other doc-value
void DocumentWriter::MergeDocvalue(Document& old_docvalue_document,
                                   Document* new_docvalue_document) {
  std::map<FieldID, IndexField*> new_docvalue_field_list;
  for (IndexField* new_field : new_docvalue_document->Fields()) {
    new_docvalue_field_list.emplace(new_field->ID(), new_field);
  }
  for (IndexField* old_field : old_docvalue_document.Fields()) {
    if (new_docvalue_field_list.find(old_field->ID()) ==
        new_docvalue_field_list.end()) {
      IndexField* new_field = new_docvalue_document->AddField();
      new_field->CopyFrom(*old_field);
    }
  }
}

// Write doc-value
SearchStatus DocumentWriter::WriteDocValue(
    const TableID& table, std::vector<DocumentUpdater*>& documents,
    WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchLogDebug("");

  Codec* codec = this->config_->GetCodec();
  SearchStatus status;
  for (DocumentUpdater* du : documents) {
    if (du->Status().OK()) {
      Document& new_document = du->New();
      Document& old_document = du->Old();
      std::string document_key;
      std::string value;
      codec->EncodeStoredFieldKey(table, new_document.ID(), document_key);

      Document& new_docvalue_document = du->NewDocValue();
      Document& old_docvalue_document = du->OldDocValue();

      new_docvalue_document.BuildDocValue(new_document);
      if (du->NeedMergeDocValue()) {
        MergeDocvalue(old_docvalue_document, &new_docvalue_document);
      }

      if (!du->Delete()) {
        bool have_field = false;
        if (new_docvalue_document.SerializeToBytes(value, 1, have_field)) {
          if (have_field) {
            // if have field ,just replace old value
            du->Status() =
                write_buffer.Put(kDocValueColumn, document_key, value);
            if (nullptr != tracer) {
              tracer->Add(TracerType::kRealInsertKeys, 1);
            }
          } else {
            // if old document have field,but we not need update.
            // need delete it
            bool need_delete_old = false;
            for (auto& old_field : old_document.Fields()) {
              if (old_field->Flag().DocValue()) {
                need_delete_old = true;
                break;
              }
            }
            if (need_delete_old) {
              du->Status() = write_buffer.Delete(kDocValueColumn, document_key);
              if (nullptr != tracer) {
                tracer->Add(TracerType::kRealInsertKeys, 1);
              }
            }
          }
        } else {
          SearchLogError("new_docvalue_document SerializeToBytes faield!!!");
          du->Status().SetStatus(kSerializeErrorStatus,
                                 "Serialize kDocValueColumn error");
        }
      } else {
        // if old document have field,but we not need update.
        // need delete it
        bool need_delete_old = false;
        for (auto& old_field : old_document.Fields()) {
          if (old_field->Flag().DocValue()) {
            need_delete_old = true;
            break;
          }
        }
        if (need_delete_old) {
          du->Status() = write_buffer.Delete(kDocValueColumn, document_key);
        }
      }
      if (!du->Status().OK()) {
        // some error happen
        status = du->Status();
        break;
      }
    }
  }

  return status;
}

// Write inverted index ,format:
// term-> doc list
// numeric-> doc list
SearchStatus DocumentWriter::WriteInvertedIndex(
    const TableID& table, std::vector<DocumentUpdater*>& documents,
    WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchLogDebug("");

  // TODO: Optimize
  Codec* codec = this->config_->GetCodec();
  SearchStatus status;
  for (auto du : documents) {
    std::unique_ptr<std::map<FieldID, TermFlagPairList>> field_terms(
        new std::map<FieldID, TermFlagPairList>());
    if (du->Status().OK()) {
      // new
      // if this is one delete operation,we do not need insert terms in
      // invert index.
      if (!du->Delete()) {
        for (auto field : du->New().Fields()) {
          // term_match pair <term_word, 1 stand for new doc word & 2 stand for
          // old doc word>
          TermFlagPairList& term_match = (*field_terms)[field->ID()];

          for (const auto& term : field->Terms()) {
            if (term.empty()) {
              continue;
            }
            SearchLogDebug("new:field[%d],doc{%lu},term{%s}", field->ID(),
                           du->New().ID(), term.c_str());
            if (field->Flag().SuffixBuild() &&
                field->FieldType() == kStringIndexField &&
                term.size() <= this->config_->GetMaxSuffixTermSize()) {
              if (field->SuffixLen() == 0) {
                SearchLogError("new doc=%lu field=%d suffix_len=%d, term=%s",
                               du->New().ID(), field->ID(), field->SuffixLen(),
                               term.c_str());
              }
              uint32_t suffix_len =
                  (field->SuffixLen() != 0)
                      ? field->SuffixLen()
                      : 5 /*Default suffix len, couldn't change this value*/;
              SearchLogDebug("invert index suffix_len=%u", suffix_len);
              UTF8SuffixBuilder builder(term.c_str(), term.size(), suffix_len);
              do {
                std::string suffix_term(builder.Term(), builder.TermSize());
                term_match[suffix_term] |= 1;
              } while (builder.Next());
            } else {
              // just as one term
              term_match[term] |= 1;
            }
          }
        }
      }

      // old
      for (auto field : du->Old().Fields()) {
        // term_match pair <term_word, 1 stand for new doc word & 2 stand for
        // old doc word>
        TermFlagPairList& term_match = (*field_terms)[field->ID()];
        for (const auto& term : field->Terms()) {
          if (term.empty()) {
            continue;
          }
          SearchLogDebug("old:field[%d],doc{%lu},term{%s}", field->ID(),
                         du->Old().ID(), term.c_str());
          if (field->Flag().SuffixBuild() &&
              field->FieldType() == kStringIndexField &&
              term.size() <= this->config_->GetMaxSuffixTermSize()) {
            if (field->SuffixLen() == 0) {
              SearchLogError("old doc=%lu field=%d suffix_len=%d, term=%s",
                             du->Old().ID(), field->ID(), field->SuffixLen(),
                             term.c_str());
            }
            uint32_t suffix_len =
                (field->SuffixLen() != 0)
                    ? field->SuffixLen()
                    : 5 /*Default suffix len, couldn't change this value*/;
            SearchLogDebug("invert index suffix_len=%u", suffix_len);
            UTF8SuffixBuilder builder(term.c_str(), term.size(), suffix_len);
            do {
              std::string suffix_term(builder.Term(), builder.TermSize());
              term_match[suffix_term] |= 1 << 1;
            } while (builder.Next());
          } else {
            term_match[term] |= 1 << 1;
          }
        }
      }

      // add or delete term
      for (auto& field : (*field_terms)) {
        const FieldID& field_id = field.first;
        for (auto& term : field.second) {
          SearchLogDebug("field[%d],doc{%lu},term{%s},hit flag{%d}", field_id,
                         du->New().ID(), term.first.c_str(), term.second);

          if (term.second == 1 || term.second == 2) {
            std::string key;
            // std::string empty_str;
            std::string value;

            codec->EncodeInvertedKey(table, field_id, term.first, key);
            DocListWriterCodec* doc_list = codec->NewOrderDocListWriterCodec();

            // 1->add 2->delete
            DocumentState s =
                term.second == 1 ? kDocumentStateOK : kDocumentStateDelete;
            doc_list->AddDocID(du->New().ID(), s);
            if (!doc_list->SerializeToBytes(value, 0)) {
              assert(false);
            }
            SearchLogDebug(
                "write_batch merge kInvertedIndexColumn key(%s), value "
                "size(%d), value/doclist[%s]",
                codec->DebugInvertedKey(key).c_str(), value.size(),
                doc_list->DebugString().c_str());
            codec->ReleaseOrderDocListWriterCodec(doc_list);

            status = write_buffer.Merge(kInvertedIndexColumn, key, value);
            if (nullptr != tracer) {
              tracer->Add(TracerType::kRealInsertKeys, 1);
            }
            if (!status.OK()) {
              // some error happen.
              return status;
            }
          }
          // else delete but add again,so do nothing.
        }
      }
    }
  }
  return status;
}

// Not support
SearchStatus DocumentWriter::WriteTableMeta(
    const TableID& table, std::vector<DocumentUpdater*>& documents,
    WriteBuffer& write_buffer, SearchTracer* tracer) {
  SearchLogDebug("");
  SearchStatus status;
  // not need
  /*
  Codec* codec = this->config_->GetCodec();
  std::string table_meta_key;
  std::string value;
  codec->EncodeMetaKey(table, table_meta_key);
  // just write empty value in current version search.
  status = write_buffer.Put(kMetaColumn, table_meta_key, value);
  return status;
  */
  return status;
}

// Not support
SearchStatus DocumentWriter::WriteDictionaryMeta(
    const TableID& table, std::vector<DocumentUpdater*>& documents,
    WriteBuffer& write_buffer, SearchTracer* tracer) {
  // Do not need global dictionary now.
  return SearchStatus();
}

// segment text with tokenizer
SearchStatus DocumentWriter::RunTokenizer(
    std::vector<DocumentUpdater*>& documents, SearchTracer* tracer) {
  SearchStatus status;
  Tokenizer* tokenizer = this->config_->GetTokenizer();
  assert(nullptr != tokenizer);
  for (auto du : documents) {
    // delete operation do not need tokenize document
    if (du->Delete()) {
      continue;
    }
    // if this document need continue,we do it.
    if (du->Status().OK()) {
      Document& document = du->New();
      auto ret = tokenizer->Do(document);
      if (!ret) {
        // some error happen? quit all document operation.
        du->Status().SetStatus(kTokenizerErrorStatus, "tokenizer error");
        status = du->Status();
        break;
      }
    }
  }
  return status;
}

// Merge old field with new field.
SearchStatus DocumentWriter::MergeOldField(
    std::vector<DocumentUpdater*>& documents, SearchTracer* tracer) {
  SearchStatus status;
  for (DocumentUpdater* du : documents) {
    // delete or replace not need merge
    if (du->UpdateType() == kDocumentDeleteType ||
        du->UpdateType() == kDocumentReplaceType) {
      continue;
    }
    Document& new_document = du->New();
    Document& old_document = du->Old();
    for (IndexField* field : old_document.Fields()) {
      // if new document do not have this field,just copy old to new
      if (nullptr == new_document.FindField(field->ID())) {
        new_document.AddField()->CopyFrom(*field);
      }
    }
  }
  return status;
}

}  // namespace wwsearch
