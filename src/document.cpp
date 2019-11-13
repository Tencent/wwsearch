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

#include "document.h"

#include "logger.h"
#include "search_store.pb.h"

namespace wwsearch {

Document::Document() : document_score_(0.0), match_field_id_(-1) {}

Document::~Document() { ClearField(); }

IndexField *Document::AddField() {
  IndexField *field = new IndexField;
  this->fields_.push_back(field);
  return field;
}

void Document::ClearField() {
  for (auto field : this->fields_) {
    delete field;
  }
  this->fields_.clear();
}

std::vector<IndexField *> &Document::Fields() { return this->fields_; }

void Document::SetID(wwsearch::DocumentID document_id) {
  this->document_id_ = document_id;
}

const DocumentID &Document::ID() const { return this->document_id_; }

void Document::SetScore(DocumentScore document_score) {
  this->document_score_ = document_score;
}

void Document::AddScore(DocumentScore document_score) {
  this->document_score_ += document_score;
}

const DocumentScore &Document::Score() const { return this->document_score_; }

void Document::BuildDocValue(Document &document) {
  this->document_id_ = document.ID();
  for (IndexField *field : document.Fields()) {
    if (field->Flag().DocValue()) {
      IndexField *new_field = new IndexField;
      new_field->CopyFrom(*field);
      this->fields_.push_back(new_field);
    }
  }
}

bool Document::SerializeToBytes(std::string &buffer, int flag,
                                bool &have_field) {
  lsmsearch::StoreDocument document;
  document.set_document_id(this->document_id_);
  bool ret = true;
  have_field = false;
  for (auto field : this->fields_) {
    // 1 => doc value
    if (1 == flag && field->Flag().DocValue()) {
      ret = field->EncodeToStoreField(document.add_fields(), 1);
      have_field = true;
    } else if (1 != flag) {
      ret = field->EncodeToStoreField(document.add_fields(), 0);
      have_field = true;
    }
    if (!ret) return ret;
  }
  ret = document.SerializeToString(&buffer);
  SearchLogDebug("SerializeToString ret = %d, document size %u, content : %s",
                 ret, buffer.size(), document.ShortDebugString().c_str());
  return ret;
}

bool Document::DeSerializeFromByte(const char *buffer, uint32_t buffer_len) {
  lsmsearch::StoreDocument document;
  bool ret = true;
  ret = document.ParseFromArray(buffer, buffer_len);
  if (!ret) {
    SearchLogError("document [ %s ]", document.ShortDebugString().c_str());
    return ret;
  }
  this->document_id_ = document.document_id();
  for (size_t i = 0; i < document.fields_size(); i++) {
    ret = this->AddField()->DecodeFromStoreField(&(document.fields(i)));
    if (!ret) return ret;
  }
  return ret;
}

DocumentUpdater::DocumentUpdater() {}

DocumentUpdater::~DocumentUpdater() {}

Document &DocumentUpdater::Old() { return this->old_document_; }

Document &DocumentUpdater::New() { return this->new_document_; }

Document &DocumentUpdater::OldDocValue() {
  return this->old_docvalue_document_;
}

Document &DocumentUpdater::NewDocValue() {
  return this->new_docvalue_document_;
}

SearchStatus &DocumentUpdater::Status() { return this->status_; }

void InvertIndexItem::Set(FieldID field_id, const std::string &term) {
  this->field_id_ = field_id;
  this->term_ = term;
  ClearDocList();
}

void InvertIndexItem::ClearDocList() { this->doc_list_.clear(); }

void InvertIndexItem::AddDocID(DocumentID doc_id) {
  this->doc_list_.insert(doc_id);
}

InvertIndexItemList::~InvertIndexItemList() {
  for (const auto *item : items_) {
    delete item;
  }
  items_.clear();
}

InvertIndexItem *InvertIndexItemList::AddItem() {
  InvertIndexItem *item = new InvertIndexItem();
  this->items_.push_back(item);
  return this->items_.back();
}

}  // namespace wwsearch
