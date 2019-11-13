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

#include "index_field.h"
#include "logger.h"

namespace wwsearch {

IndexFieldFlag::~IndexFieldFlag() {}

// Set need segment text.
void IndexFieldFlag::SetTokenize() { this->flag_ |= kTokenizeFieldFlag; }

// If open tokenize ?
bool IndexFieldFlag::Tokenize() const {
  return this->flag_ & kTokenizeFieldFlag;
}

// Set store document's field.
void IndexFieldFlag::SetStoredField() { this->flag_ |= kStoreFieldFlag; }

// If store ?
bool IndexFieldFlag::StoredField() const {
  return this->flag_ & kStoreFieldFlag;
}

// Set store doc-value
void IndexFieldFlag::SetDocValue() { this->flag_ |= kDocValueFieldFlag; }

// If store?
bool IndexFieldFlag::DocValue() const {
  return this->flag_ & kDocValueFieldFlag;
}

// Set suffix build.
// If open,term will split into multi terms.
// exmaple word "hello",will split into:
// "hello","ello","llo","lo","o"
void IndexFieldFlag::SetSuffixBuild() { this->flag_ |= kSuffixBuildFlag; }

// If open?
bool IndexFieldFlag::SuffixBuild() const {
  return this->flag_ & kSuffixBuildFlag;
}

// Set store inverted index.
void IndexFieldFlag::SetInvertIndex() { this->flag_ |= kInvertIndexFieldFlag; }

// If open?
bool IndexFieldFlag::InvertIndex() const {
  return this->flag_ & kInvertIndexFieldFlag;
}

// build index,but not store it in fields
// One optimze api
void IndexFieldFlag::SetNotStoreInvertTerm() {
  this->flag_ |= kNotStoreInvertTermFieldFlag;
}

// If open?
bool IndexFieldFlag::NotStoreInvertTerm() const {
  return this->flag_ & kNotStoreInvertTermFieldFlag;
}

IndexField::IndexField()
    : field_id_(-1),
      field_type_(kIndexFieldUnknowType),
      numeric_value_(0),
      string_value_(""),
      suffix_len_(0),
      use_outer_segment_terms_(false) {}

IndexField::~IndexField() {}

void IndexField::CopyFrom(const IndexField& obj) {
  this->field_id_ = obj.field_id_;
  this->field_flag_ = obj.field_flag_;
  this->field_type_ = obj.field_type_;
  this->numeric_value_ = obj.numeric_value_;
  this->string_value_ = obj.string_value_;
  this->use_outer_segment_terms_ = obj.use_outer_segment_terms_;
  this->terms_ = obj.terms_;
  this->suffix_len_ = obj.suffix_len_;
  this->numeric_list_.assign(obj.numeric_list_.begin(),
                             obj.numeric_list_.end());
}

void IndexField::SetSuffixLen(uint32_t suffix_len) {
  this->suffix_len_ = suffix_len;
}

// uint32 field
void IndexField::SetUint32(uint32_t value) {
  this->field_type_ = kUint32IndexField;
  this->numeric_value_ = value;
}

// uint53 field.
void IndexField::SetUint64(uint64_t value) {
  this->field_type_ = kUint64IndexField;
  this->numeric_value_ = value;
}

// string field
void IndexField::SetString(const std::string& value) {
  this->field_type_ = kStringIndexField;
  this->string_value_ = value;
}

// numeric list field
void IndexField::SetNumericList(const std::vector<uint64_t>& numeric_list) {
  this->field_type_ = kUint64IndexField;
  this->numeric_list_.assign(numeric_list.begin(), numeric_list.end());
}

// api
bool IndexField::SerializeToBytes(std::string& buffer, int flag) {
  return true;
}

// api
bool IndexField::DeSerializeFromByte(const char* buffer, uint32_t buffer_len) {
  return true;
}

// encod stored field to bytes stream.
bool IndexField::EncodeToStoreField(lsmsearch::StoreIndexField* field,
                                    int flag) {
  field->set_field_id(this->field_id_);
  field->set_field_flag(this->field_flag_.Flag());
  field->set_field_type(this->field_type_);

  // 0 -> stored field
  // 1 -> doc value (only store value,but not term list)
  bool need_value = false;
  if (0 == flag) {
    need_value = this->field_flag_.StoredField();
  } else {
    need_value = this->field_flag_.DocValue();
  }

  if (need_value) {
    switch (this->field_type_) {
      case kStringIndexField:
        field->set_string_value(this->string_value_);
        SearchLogDebug("setting string_value : %s, target %s",
                       this->string_value_.c_str(),
                       field->string_value().c_str());
        break;
      case kUint32IndexField:
      case kUint64IndexField:
        field->set_numeric_value(this->numeric_value_);
        break;
      default:
        break;
    }
  }

  // If we do not need store invert terms for some case.
  bool not_store_invert_terms = this->field_flag_.NotStoreInvertTerm();
  if (0 == flag && !not_store_invert_terms) {
    SearchLogDebug("store_invert_terms true!!!");
    for (std::set<std::string>::iterator it = this->terms_.begin();
         it != this->terms_.end(); ++it) {
      const std::string& term = *it;
      std::string* ptr = field->add_terms();
      *ptr = term;
    }
  }

  if (this->suffix_len_ != 0) {
    field->set_suffix_len(this->suffix_len_);
  }
  SearchLogDebug("EncodeToStoreField field pb {%s}",
                 field->ShortDebugString().c_str());
  return true;
}

// Decored stored field from bytes stream.
bool IndexField::DecodeFromStoreField(const lsmsearch::StoreIndexField* field) {
  SearchLogDebug("DecodeFromStoreField field pb {%s}, index_field addr {%p}",
                 field->ShortDebugString().c_str(), this);
  this->field_id_ = field->field_id();
  this->field_flag_.SetFlag(field->field_flag());
  this->field_type_ = (kIndexFieldType)(field->field_type());

  bool need_value =
      this->field_flag_.StoredField() || this->field_flag_.DocValue();
  if (need_value) {
    switch (this->field_type_) {
      case kStringIndexField:
        this->string_value_ = field->string_value();
        SearchLogDebug("field_id {%u}, string_value_ {%s}", this->field_id_,
                       this->string_value_.c_str());
        break;
      case kUint32IndexField:
      case kUint64IndexField:
        this->numeric_value_ = field->numeric_value();
        break;
      default:
        break;
    }
  }

  SearchLogDebug(
      "IndexField::DecodeFromStoreField1 terms size {%d}, this->terms size "
      "{%d}",
      field->terms_size(), this->terms_.size());
  // keep order
  for (size_t i = 0; i < field->terms_size(); i++) {
    this->terms_.insert(field->terms(i));
  }
  if (this->field_flag_.SuffixBuild()) {
    if (field->has_suffix_len()) {
      this->suffix_len_ = field->suffix_len();
    }
    if (this->suffix_len_ == 0) {
      this->suffix_len_ = 5 /*Default suffix len, couldn't change this value*/;
    }
  }
  SearchLogDebug(
      "IndexField::DecodeFromStoreField2 terms size {%d}, this->terms size "
      "{%d}",
      field->terms_size(), this->terms_.size());
  return true;
}

}  // namespace wwsearch
