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
#include <algorithm>
#include <sstream>
#include "header.h"
#include "index_field.h"
#include "logger.h"
#include "utils.h"

namespace wwsearch {

// Basic filter api
class Filter {
 protected:
  IndexField field_;

 public:
  virtual ~Filter() {}

  // Must Get it and set
  IndexField *GetField() { return &field_; }

  // match this filter ?
  virtual bool Match(const IndexField *field) = 0;

  inline FieldID GetFieldID() { return field_.ID(); }

  virtual std::string PrintReadableStr() {
    return std::string("not implementation");
  };
  // If field match uint32 or uint64 field.
  static bool CheckFieldTypeNumeric(const IndexField &field) {
    if (field.FieldType() == kUint32IndexField ||
        field.FieldType() == kUint64IndexField) {
      return true;
    }
    return false;
  };
  // If field match string field.
  static bool CheckFieldTypeString(const IndexField &field) {
    if (field.FieldType() == kStringIndexField) {
      return true;
    }
    return false;
  };

 private:
};

// Equal api ,spport numeric and string.
class EqualFilter : public Filter {
 private:
 public:
  virtual ~EqualFilter() {}

  virtual bool Match(const IndexField *field) override {
    // if no field stored,just filter it,because we do not know
    if (nullptr == field) return false;
    assert(field->ID() == field_.ID());

    if (CheckFieldTypeNumeric(*field) && CheckFieldTypeNumeric(field_)) {
      return field_.NumericValue() == field->NumericValue();
    } else if (CheckFieldTypeString(*field) && CheckFieldTypeString(field_)) {
      return field_.StringValue() == field->StringValue();
    }
    // type not match.skip
    return false;
  }

  virtual std::string PrintReadableStr() {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "field=%u equal [%llu]", field_.ID(),
             field_.NumericValue());
    return std::string(buffer);
  }

 private:
};

// Not equal filter,support numeric and string field.
class NotEqualFilter : public Filter {
 private:
 public:
  virtual ~NotEqualFilter() {}

  virtual bool Match(const IndexField *field) override {
    // if no field stored,just filter it,because we do not know
    if (nullptr == field) return false;
    assert(field->ID() == field_.ID());

    if (CheckFieldTypeNumeric(*field) && CheckFieldTypeNumeric(field_)) {
      return field_.NumericValue() != field->NumericValue();
    } else if (CheckFieldTypeString(*field) && CheckFieldTypeString(field_)) {
      return field_.StringValue() != field->StringValue();
    }
    // type not match,skip
    return false;
  }
  virtual std::string PrintReadableStr() {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "field=%u not equal [%llu]", field_.ID(),
             field_.NumericValue());
    return std::string(buffer);
  }

 private:
};

// Range field, only support numeric field.
class RangeFilter : public Filter {
 private:
  uint64_t begin_;
  uint64_t end_;

 public:
  RangeFilter(uint64_t begin, uint64_t end) : begin_(begin), end_(end) {}

  virtual ~RangeFilter() {}

  virtual bool Match(const IndexField *field) override {
    // if no field stored,just filter it,because we do not know
    if (nullptr == field) return false;
    assert(field->ID() == field_.ID());

    if (!CheckFieldTypeNumeric(*field) /*|| !CheckFieldTypeNumeric(field_)*/) {
      return false;
    }

    return field->NumericValue() >= begin_ && field->NumericValue() <= end_;
  }

  virtual std::string PrintReadableStr() {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "field=%u in range [%llu,%llu]",
             field_.ID(), begin_, end_);
    return std::string(buffer);
  }

 private:
};

// If one value match the field?
class InFilter : public Filter {
 private:
 public:
  virtual ~InFilter() {}

  virtual bool Match(const IndexField *field) override {
    // if no field stored,just filter it,because we do not know
    if (nullptr == field) return false;
    assert(field->ID() == field_.ID());
    // must be string
    if (field_.FieldType() == kStringIndexField &&
        field->FieldType() == kStringIndexField) {
      SearchLogDebug("doc field=%s, query filter=%s",
                     field_.StringValue().c_str(),
                     field->StringValue().c_str());
      return field->StringValue().find(field_.StringValue()) !=
             std::string::npos;
    } else {
      return false;
    }
  }

  virtual std::string PrintReadableStr() {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "field=%u in [%s]", field_.ID(),
             field_.StringValue().c_str());
    return std::string(buffer);
  }

 private:
};

// If no one value match the field?
class NotInFilter : public Filter {
 private:
 public:
  virtual ~NotInFilter() {}

  virtual bool Match(const IndexField *field) override {
    // if no field stored,just filter it,because we do not know
    if (nullptr == field) return false;
    assert(field->ID() == field_.ID());
    // must be string
    if (field_.FieldType() == kStringIndexField &&
        field->FieldType() == kStringIndexField) {
      return field->StringValue().find(field_.StringValue()) ==
             std::string::npos;
    } else {
      return false;
    }
  }

  virtual std::string PrintReadableStr() {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "field=%u not in [%s]", field_.ID(),
             field_.StringValue().c_str());
    return std::string(buffer);
  }

 private:
};

// If one numeric value match the field?
class InNumericListFilter : public Filter {
 private:
 public:
  virtual ~InNumericListFilter() {}

  virtual bool Match(const IndexField *field) override {
    if (nullptr == field) return false;
    assert(field->ID() == field_.ID());

    if (!CheckFieldTypeNumeric(*field) || !CheckFieldTypeNumeric(field_)) {
      return false;
    }
    const std::vector<uint64_t> &numeric_list = field_.NumericList();
    uint64_t numeric_value = field->NumericValue();
    auto iter =
        std::find(numeric_list.begin(), numeric_list.end(), numeric_value);
    return iter != numeric_list.end();
  }

  virtual std::string PrintReadableStr() {
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "field=%u in [%s]", field_.ID(),
             JoinContainerToString(field_.NumericList(), ";").c_str());
    return std::string(buffer);
  }

 private:
};

// If no one value match the field?
class NotInNumericListFilter : public Filter {
 private:
 public:
  virtual ~NotInNumericListFilter() {}

  virtual bool Match(const IndexField *field) override {
    if (nullptr == field) return false;
    assert(field->ID() == field_.ID());

    if (!CheckFieldTypeNumeric(*field) || !CheckFieldTypeNumeric(field_)) {
      return false;
    }
    const std::vector<uint64_t> &numeric_list = field_.NumericList();
    uint64_t numeric_value = field->NumericValue();
    auto iter =
        std::find(numeric_list.begin(), numeric_list.end(), numeric_value);
    return iter == numeric_list.end();
  }

  virtual std::string PrintReadableStr() {
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "field=%u in [%s]", field_.ID(),
             JoinContainerToString(field_.NumericList(), ";").c_str());
    return std::string(buffer);
  }

 private:
};

// If some string match the field?
class MatchStringListFilter : public Filter {
 private:
  std::vector<std::string> string_value_list_;
  bool revert_;
  uint32_t min_should_match_filter_values_num_;

 public:
  MatchStringListFilter(
      const std::vector<std::string> &string_list, bool revert = false,
      uint32_t min_should_match_filter_values_num = 0xFFFFFFFF)
      : string_value_list_(string_list),
        revert_(revert),
        min_should_match_filter_values_num_(
            min_should_match_filter_values_num) {
    min_should_match_filter_values_num_ =
        (min_should_match_filter_values_num_ > string_value_list_.size())
            ? string_value_list_.size()
            : min_should_match_filter_values_num_;
  }
  virtual ~MatchStringListFilter() {}

  virtual bool Match(const IndexField *field) override {
    if (nullptr == field) return false;
    assert(field->ID() == field_.ID());
    if (field->FieldType() != kStringIndexField) {
      return false;
    }

    bool match = InnerMatch(field);
    return revert_ ? !match : match;
  }

  virtual std::string PrintReadableStr() {
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "MatchStringListFilter: size=%u revert=%d",
             string_value_list_.size(), revert_);
    return std::string(buffer);
  }

 private:
  std::string ParseStringValue(const std::string &field_str_val,
                               const std::string &string_value) {
    if (field_str_val.size() != 4 || string_value.size() % 4 != 0) {
      return std::string("WTF???? string value size not 4*x\n");
    }

    std::ostringstream o;
    uint32_t hash_val = *(uint32_t *)field_str_val.c_str();
    hash_val = ntohl(hash_val);
    o << "field hash_val = " << hash_val << ", ";

    for (int i = 0; i < string_value.size(); i += 4) {
      std::string sub = string_value.substr(i, 4);
      hash_val = *(uint32_t *)sub.c_str();
      hash_val = ntohl(hash_val);
      o << hash_val << " ";
    }
    return o.str();
  }

  bool InnerMatch(const IndexField *field) {
    uint32_t match_count = 0;
    for (size_t i = 0; i < string_value_list_.size(); i++) {
      if (field->StringValue().find(string_value_list_[i]) !=
          std::string::npos) {
        match_count++;
      }
    }
    return match_count >= min_should_match_filter_values_num_;
  }
};

}  // namespace wwsearch
