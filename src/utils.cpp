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

#include "utils.h"
#include "codec.h"
#include "codec_doclist_impl.h"
#include "search_slice.h"

namespace wwsearch {
std::string DebugInvertedValue(const std::string& value) {
  // Not support now.
  return "";
  assert(false);
  // little end
  // data header : flag, 1B
  // data content : doc_id+doc_state pair, occupy size = 8B+1B
  // data : <doc_id_state_pair>,<doc_id_state_pair>,<doc_id_state_pair>....

  return "";
  assert(false);

  static int occupy_size = sizeof(uint64_t) + sizeof(uint8_t);
  assert((value.size() - sizeof(uint8_t)) % occupy_size == 0);

  std::ostringstream o;

  // only work for litte end
  wwsearch::Slice slice(value.c_str(), value.size());
  uint8_t doc_list_header_flag = *((uint8_t*)slice.data());
  slice.remove_prefix(sizeof(uint8_t));
  o << "slice_size(" << slice.size()
    << ") DocListHeader flag = " << unsigned(doc_list_header_flag) << ",";
  while (!slice.empty()) {
    uint64_t doc_id = *((uint64_t*)slice.data());
    slice.remove_prefix(sizeof(uint64_t));
    uint8_t doc_state = *((uint8_t*)slice.data());
    slice.remove_prefix(sizeof(uint8_t));
    o << "<doc_id=" << doc_id << ",doc_state=" << unsigned(doc_state) << ">,";
  }

  return o.str();
}

std::string DebugInvertedValueByReader(Codec* codec, const std::string& value) {
  std::ostringstream o;

  DocListReaderCodec* doc_list_reader =
      codec->NewDocListReaderCodec(value.c_str(), value.size());
  while (doc_list_reader->DocID() != DocIdSetIterator::NO_MORE_DOCS) {
    uint64_t doc_id = static_cast<uint64_t>(doc_list_reader->DocID());
    uint8_t doc_state = doc_list_reader->State();
    o << "<doc_id=" << doc_id << ",doc_state=" << unsigned(doc_state) << ">,";
    doc_list_reader->NextDoc();
  }
  codec->ReleaseDocListReaderCodec(doc_list_reader);
  return o.str();
}

void SplitString(const std::string& full, const std::string& delim,
                 std::vector<std::string>* result) {
  result->clear();
  if (full.empty()) {
    return;
  }

  std::string tmp;
  std::string::size_type pos_begin = full.find_first_not_of(delim);
  std::string::size_type comma_pos = 0;

  while (pos_begin != std::string::npos) {
    comma_pos = full.find(delim, pos_begin);
    if (comma_pos != std::string::npos) {
      tmp = full.substr(pos_begin, comma_pos - pos_begin);
      pos_begin = comma_pos + delim.length();
    } else {
      tmp = full.substr(pos_begin);
      pos_begin = comma_pos;
    }

    if (!tmp.empty()) {
      result->push_back(tmp);
      tmp.clear();
    }
  }
}

std::string TrimString(const std::string& str, const std::string& trim) {
  std::string::size_type pos = str.find_first_not_of(trim);
  if (pos == std::string::npos) {
    return str;
  }
  std::string::size_type pos2 = str.find_last_not_of(trim);
  if (pos2 != std::string::npos) {
    return str.substr(pos, pos2 - pos + 1);
  }
  return str.substr(pos);
}

}  // namespace wwsearch
