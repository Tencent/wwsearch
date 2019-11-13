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

#include "unittest_util.h"

namespace wwsearch {

void InitStringField(IndexField *field, uint16_t field_id,
                     const std::string &word) {
  IndexFieldFlag flag;
  flag.SetDocValue();
  flag.SetStoredField();
  flag.SetTokenize();
  flag.SetInvertIndex();
  field->SetMeta(field_id, flag);
  field->SetString(word);
}

void InitUint32Field(IndexField *field, uint16_t field_id, uint32_t value) {
  IndexFieldFlag flag;
  flag.SetDocValue();
  flag.SetStoredField();
  flag.SetTokenize();
  flag.SetInvertIndex();
  field->SetMeta(field_id, flag);
  field->SetUint32(value);
}

void InitUint64Field(IndexField *field, uint16_t field_id, uint64_t value) {
  IndexFieldFlag flag;
  flag.SetDocValue();
  flag.SetStoredField();
  flag.SetTokenize();
  flag.SetInvertIndex();

  field->SetMeta(field_id, flag);
  field->SetUint64(value);
}

DocumentUpdater *TestUtil::NewDocument(DocumentID documentid,
                                       std::string str_field,
                                       uint32_t numeric_32, uint64_t numeric_64,
                                       uint32_t sort_value,
                                       uint8_t field_offset) {
  DocumentUpdater *du = new DocumentUpdater();
  Document &document = du->New();
  document.SetID(documentid);

  InitStringField(document.AddField(), field_offset + 1, str_field);
  InitUint32Field(document.AddField(), field_offset + 2, numeric_32);
  InitUint32Field(document.AddField(), field_offset + 3, numeric_64);
  InitUint32Field(document.AddField(), field_offset + 4, sort_value);
  return du;
}

DocumentUpdater *TestUtil::NewStringFieldDocument(
    DocumentID documentid,
    const std::vector<TestUtil::FieldIdStrPair> &field_id_str_list) {
  DocumentUpdater *du = new DocumentUpdater();
  Document &document = du->New();
  document.SetID(documentid);
  for (auto &field_id_str : field_id_str_list) {
    InitStringField(document.AddField(), field_id_str.first,
                    field_id_str.second);
  }
  return du;
}

}  // namespace wwsearch
