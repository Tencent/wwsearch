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

namespace wwsearch {

enum kSortConditionType { kSortConditionDesc = 1, kSortConditionAsc = 2 };

class SortCondition {
 protected:
  FieldID field_id_;
  kSortConditionType sort_type_;

 public:
  SortCondition(FieldID field_id, kSortConditionType sort_type)
      : field_id_(field_id), sort_type_(sort_type) {}

  virtual ~SortCondition() {}

  virtual bool Greater(const DocumentID &d1, const IndexField *f1,
                       const DocumentID &d2, const IndexField *f2) const = 0;

  inline FieldID GetID() { return this->field_id_; }

 private:
};

// NumericSortCondition
class NumericSortCondition : public SortCondition {
 private:
 public:
  NumericSortCondition(FieldID field_id, kSortConditionType sort_type)
      : SortCondition(field_id, sort_type) {}

  virtual ~NumericSortCondition() {}

  virtual bool Greater(const DocumentID &d1, const IndexField *f1,
                       const DocumentID &d2, const IndexField *f2) const {
    auto ret = InnerGreater(d1, f1, d2, f2);
    return (kSortConditionDesc == sort_type_) ? ret : !ret;
  }

  // have field first
  // document id big first
  bool InnerGreater(const DocumentID &d1, const IndexField *f1,
                    const DocumentID &d2, const IndexField *f2) const {
    const IndexField *nf1 =
        (f1 != nullptr && (f1->FieldType() == kUint32IndexField ||
                           f1->FieldType() == kUint64IndexField))
            ? f1
            : nullptr;
    const IndexField *nf2 =
        (f2 != nullptr && (f2->FieldType() == kUint32IndexField ||
                           f2->FieldType() == kUint64IndexField))
            ? f2
            : nullptr;

    if (nullptr == nf1 && nullptr == nf2) {
      return d1 > d2;
    } else if (nullptr == nf1) {
      return false;
    } else if (nullptr == nf2) {
      return true;
    }

    return nf1->NumericValue() > nf2->NumericValue();
  }

 private:
};

class StringSortCondition : public SortCondition {
 private:
 public:
  StringSortCondition(FieldID field_id, kSortConditionType sort_type)
      : SortCondition(field_id, sort_type) {}

  virtual ~StringSortCondition() {}

  virtual bool Greater(const DocumentID &d1, const IndexField *f1,
                       const DocumentID &d2, const IndexField *f2) const {
    auto ret = InnerGreater(d1, f1, d2, f2);
    return (kSortConditionDesc == sort_type_) ? ret : !ret;
  }

  // have field first
  // document id big first
  bool InnerGreater(const DocumentID &d1, const IndexField *f1,
                    const DocumentID &d2, const IndexField *f2) const {
    const IndexField *nf1 =
        (f1 != nullptr && f1->FieldType() == kStringIndexField) ? f1 : nullptr;
    const IndexField *nf2 =
        (f2 != nullptr && f2->FieldType() == kStringIndexField) ? f2 : nullptr;

    if (nullptr == nf1 && nullptr == nf2) {
      return d1 > d2;
    } else if (nullptr == nf1) {
      return false;
    } else if (nullptr == nf2) {
      return true;
    }

    size_t min_len = (nf1->StringValue().size() < nf2->StringValue().size()
                          ? nf1->StringValue().size()
                          : nf2->StringValue().size());
    int ret =
        memcmp(nf1->StringValue().c_str(), nf2->StringValue().c_str(), min_len);
    if (0 == ret) {
      return nf1->StringValue().size() > nf2->StringValue().size();
    }

    return ret > 0;
  }

 private:
};

class Sorter {
 private:
  std::vector<SortCondition *> *sort_condictions_;

 public:
  Sorter(std::vector<SortCondition *> *sort_condictions = nullptr)
      : sort_condictions_(sort_condictions) {}

  virtual ~Sorter() {}

  // do lhs should first pop ?
  inline bool operator()(Document *&lhs, Document *&rhs) const {
    if (nullptr == sort_condictions_) return lhs->ID() > rhs->ID();
    for (auto sc : *sort_condictions_) {
      auto lhs_field = lhs->FindField(sc->GetID());
      auto rhs_field = rhs->FindField(sc->GetID());
      auto c1 = sc->Greater(lhs->ID(), lhs_field, rhs->ID(), rhs_field);
      auto c2 = sc->Greater(rhs->ID(), rhs_field, lhs->ID(), lhs_field);
      if (c1 && c2) {
        continue;
      } else if (c1) {
        return true;
      } else {
        return false;
      }
    }
    // default sort
    // return lhs->ID() < rhs->ID();
    return lhs->ID() > rhs->ID();
  }

 private:
};

}  // namespace wwsearch
