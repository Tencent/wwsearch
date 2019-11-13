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

#include "index_field.h"
#include "search_status.h"
#include "serialize.h"
#include "storage_type.h"

namespace wwsearch {

struct DocumentMeta {
  uint64_t total_documents;
  uint64_t delete_documents;
  uint64_t increase_seq;
  uint64_t terms_count;
  void Clear() {
    total_documents = delete_documents = increase_seq = terms_count = 0;
  }
} __attribute__((__packed__));

typedef struct DocumentMeta DocumentMeta;

class Document /*: public SerializeAble*/ {
 private:
  std::vector<IndexField *> fields_;
  DocumentID document_id_;

  // Only use for post_score in collector_top.cpp
  int match_field_id_;
  DocumentScore document_score_;

 public:
  Document();

  ~Document();

  Document(const Document &) = delete;
  Document &operator=(const Document &) = delete;

  // Do not add same field with same field id.
  IndexField *AddField();

  void ClearField();

  inline IndexField *FindField(FieldID field_id) {
    for (size_t i = 0; i < fields_.size(); i++) {
      if (field_id == fields_[i]->ID()) {
        return fields_[i];
      }
    }
    return nullptr;
  }

  std::vector<IndexField *> &Fields();

  void SetID(DocumentID document_id);

  const DocumentID &ID() const;

  void SetMatchFieldID(int match_field_id) { match_field_id_ = match_field_id; }

  int MatchFieldId() { return match_field_id_; }

  void SetScore(DocumentScore document_score);

  void AddScore(DocumentScore document_score);

  const DocumentScore &Score() const;

  // if flag = 0,all will be srialized.
  // if flag = 1,only doc value field will be serialized.
  bool SerializeToBytes(std::string &buffer, int flag, bool &have_field);

  bool SerializeToBytes(std::string &buffer, int flag) {
    bool have_field = false;
    return SerializeToBytes(buffer, flag, have_field);
  }

  bool DeSerializeFromByte(const char *buffer, uint32_t buffer_len);

  void BuildDocValue(Document &document);

  void PrintToReadStr(std::string &str) {
    char buffer[20];
    snprintf(buffer, sizeof(buffer), "Document[%llu]\n", document_id_);
    str.append(buffer);
    for (auto field : fields_) {
      field->PrintToString(str);
    }
  }

  bool EncodeToPBDocument(lsmsearch::StoreDocument *document) {
    document->set_document_id(this->document_id_);
    for (auto field : this->fields_) {
      if (!field->EncodeToStoreField(document->add_fields())) return false;
    }
    return true;
  }

 private:
};

class IndexWriter;
enum kDocumentUpdaterType {
  kDocumentAddType = 1,
  kDocumentUpdateType = 2,
  kDocumentAddOrUpdateType = 3,
  kDocumentDeleteType = 4,
  kDocumentReplaceType = 5,
  kDocumentAddWithoutReadType = 6  // Just add into index but not read
};
class DocumentUpdater {
  friend class IndexWriter;

 private:
  SearchStatus status_;
  Document old_document_;
  Document new_document_;
  Document old_docvalue_document_;
  Document new_docvalue_document_;
  kDocumentUpdaterType update_type_;  // inner use

 public:
  DocumentUpdater();

  ~DocumentUpdater();

  DocumentUpdater(const DocumentUpdater &) = delete;
  DocumentUpdater &operator=(const DocumentUpdater &) = delete;

  Document &Old();

  Document &New();

  Document &OldDocValue();

  Document &NewDocValue();

  SearchStatus &Status();

  bool Delete() { return update_type_ == kDocumentDeleteType; }

  bool NeedMergeDocValue() {
    return (kDocumentUpdateType == update_type_) ||
           (kDocumentAddOrUpdateType == update_type_);
  }

  kDocumentUpdaterType UpdateType() { return this->update_type_; }

 private:
  void SetUpdateType(kDocumentUpdaterType type) { this->update_type_ = type; }
};

class InvertIndexItem {
 private:
  FieldID field_id_;
  std::string term_;
  std::set<DocumentID, std::greater<DocumentID>>
      doc_list_;  // in decrease order.

 public:
  InvertIndexItem() {}
  virtual ~InvertIndexItem() {}

  FieldID GetFieldID() const { return this->field_id_; }
  const std::string &GetTerm() const { return this->term_; }
  void Set(FieldID field_id, const std::string &term);
  void ClearDocList();
  void AddDocID(DocumentID doc_id);
  inline const std::set<DocumentID, std::greater<DocumentID>> &DocList() const {
    return this->doc_list_;
  }

 private:
};

class InvertIndexItemList {
 private:
  std::list<InvertIndexItem *> items_;

 public:
  InvertIndexItemList() {}

  virtual ~InvertIndexItemList();

  InvertIndexItem *AddItem();

  std::list<InvertIndexItem *> List() { return this->items_; }

 private:
};

}  // namespace wwsearch
