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

#include "tokenizer_mmseg.h"
#include "logger.h"

#ifdef WIN32
#include "bsd_getopt_win.h"
#else
#include "bsd_getopt.h"
#endif

#include "Segmenter.h"
#include "SegmenterManager.h"
#include "SynonymsDict.h"
#include "ThesaurusDict.h"
#include "UnigramCorpusReader.h"
#include "UnigramDict.h"
#include "csr_utils.h"

#include "checked.h"
#include "codec.h"
#include "logger.h"
#include "unchecked.h"
#include "utf8_suffixbuilder.h"

using namespace std;
using namespace css;

#define SEGMENT_OUTPUT 1
namespace wwsearch {

// only support ascii char now.
bool TokenizerMMSEG::Do(wwsearch::Document& document) {
  SearchLogDebug("");
  uint32_t idx = document.ID() % this->manager_num_;
  this->hash_lock_->Lock(idx);
  SegmenterManager* manager = this->managers_[idx];
  // WTF of mmseg mem leak.
  Segmenter* seg = manager->getSegmenter(false);
  for (auto field : document.Fields()) {
    if (!field->Flag().InvertIndex()) {
      continue;
    }
    SearchLogDebug(
        "TokenizerMMSEG::Do field id:%d,type:%d,flag:%d,use outer:%d,term:%lu",
        field->ID(), field->FieldType(), field->Flag().Flag(),
        field->UseOuterSegmentTerms(), field->Terms().size());
    if (field->UseOuterSegmentTerms()) {
      // Special case: if user have segment terms before ,we will skip the job.
      SearchLogDebug("user have segment terms,skip it.");
      continue;
    }
    if (field->FieldType() == kStringIndexField && field->Flag().Tokenize()) {
      const std::string& str = field->StringValue();
      seg->setBuffer((u1*)str.c_str(), str.size());
      u2 len = 0, symlen = 0;
      u2 kwlen = 0, kwsymlen = 0;
      while (1) {
        len = 0;
        char* tok = (char*)seg->peekToken(len, symlen);
        if (!tok || !*tok || !len) break;
        seg->popToken(len);
        // skip
        if (*tok == '\r' || *tok == '\n') continue;

        // get term
        std::string term(tok, len);
        ToLowerCase(term);
        field->Terms().insert(term);
      }
    } else {
      std::string term;
      if (field->FieldType() == kStringIndexField) {
        term = field->StringValue();
        ToLowerCase(term);
      } else if (field->FieldType() == kUint32IndexField) {
        AppendFixed32(term, field->NumericValue());
      } else if (field->FieldType() == kUint64IndexField) {
        AppendFixed64(term, field->NumericValue());
      }
      if (!term.empty()) field->Terms().insert(term);
    }
  }
  delete seg;

  this->hash_lock_->unlock(idx);
  return true;
}

bool TokenizerMMSEG::BuildTerms(const char* buffer, size_t buffer_size,
                                std::set<std::string>& terms,
                                bool no_covert_to_lower_case) {
  std::uint64_t idx =
      seq_.fetch_add(1, std::memory_order_relaxed) % this->manager_num_;
  this->hash_lock_->Lock(idx);
  SegmenterManager* manager = this->managers_[idx];
  Segmenter* seg = manager->getSegmenter(false);

  seg->setBuffer((u1*)buffer, buffer_size);
  u2 len = 0, symlen = 0;
  u2 kwlen = 0, kwsymlen = 0;
  while (1) {
    len = 0;
    char* tok = (char*)seg->peekToken(len, symlen);
    if (!tok || !*tok || !len) break;
    seg->popToken(len);
    // skip
    if (*tok == '\r' || *tok == '\n') continue;

    // get term
    std::string term(tok, len);
    if (no_covert_to_lower_case == false) {
      ToLowerCase(term);
    }
    terms.insert(term);
  }
  delete seg;

  this->hash_lock_->unlock(idx);
  return true;
}

void TokenizerMMSEG::ToLowerCase(std::string& old) {
  if (old.empty()) return;

  std::string new_str = old.substr();
  SearchLogDebug("before lower %s", old.c_str());
  unsigned char *prev, *it, *tail;
  it = (unsigned char*)new_str.c_str();
  tail = (unsigned char*)new_str.c_str() + new_str.size();

  do {
    try {
      prev = it;
      utf8::next(it, tail);
      if (it - prev == 1) {
        if (*prev >= 'A' && *prev <= 'Z') {
          SearchLogDebug("to lower case %c", *prev);
          *prev = 'a' + (*prev - 'A');
        }
      }
    } catch (...) {
      // error ?
      break;
    }
  } while (it < tail);
  SearchLogDebug("after lower %s", old.c_str());
  old = std::move(new_str);
}

}  // namespace wwsearch
