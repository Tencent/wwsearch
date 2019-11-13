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

#include "tokenizer_impl.h"
#include "codec.h"
#include "logger.h"

namespace wwsearch {

// only support ascii char now.
bool TokenizerSpaceImpl::Do(wwsearch::Document &document) {
  SearchLogDebug("");

#define SPACE_TAG ' '
  for (auto field : document.Fields()) {
    SearchLogDebug("field id:%d,type:%d,flag:%d", field->ID(),
                   field->FieldType(), field->Flag().Flag());
    if (field->Flag().InvertIndex()) {
      //  case 1: string and need tokenize
      //  case 2: the whole value will encode as one term,including
      //  string/numeric value
      if (field->FieldType() == kStringIndexField && field->Flag().Tokenize()) {
        const std::string &str = field->StringValue();
        if (!BuildTerms(str.c_str(), str.size(), field->Terms())) {
          return false;
        }
      } else {
        std::string term;
        if (field->FieldType() == kStringIndexField) {
          term = field->StringValue();
        } else if (field->FieldType() == kUint32IndexField) {
          AppendFixed32(term, field->NumericValue());
        } else if (field->FieldType() == kUint64IndexField) {
          AppendFixed64(term, field->NumericValue());
        }
        if (!term.empty()) field->Terms().insert(term);
      }
    }
  }
  return true;
}

bool TokenizerSpaceImpl::BuildTerms(const char *buffer, size_t buffer_size,
                                    std::set<std::string> &terms,
                                    bool no_covert_to_lower_case) {
  if (buffer_size > 0) {
    std::string::size_type prev, curr;
    prev = 0;
    for (;;) {
      //
      while (prev < buffer_size && IsSkipChar(buffer[prev])) {
        prev++;
      }
      if (prev >= buffer_size) break;

      //
      curr = prev;
      while (curr < buffer_size && !IsSkipChar(buffer[curr])) {
        curr++;
      }

      std::string term(buffer + prev, curr - prev);

      SearchLogDebug("tokenizer build term {%s}", term.c_str());
      terms.insert(std::move(term));
      prev = curr;
    }
  }
  return true;
}

}  // namespace wwsearch
