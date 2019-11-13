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

#include "doclist_compression.h"
#include "logger.h"

namespace wwsearch {

bool DocListDelDeltaBuffer::AddDeletePos(size_t pos) {
  if (!del_position_.empty()) {
    if (pos <= last_pos_) return false;
  }
  del_position_.push_back(pos);
  last_pos_ = pos;
  del_num_++;
  return true;
}

bool DocListDelDeltaBuffer::SerializeToString(std::string &buffer) {
  if (del_num_ != 0) {
    PutVarint32(&buffer, del_num_);
    assert(del_num_ == del_position_.size());
    size_t prev_pos = 0;
    for (const auto &pos : del_position_) {
      PutVarint32(&buffer, pos - prev_pos);
      prev_pos = pos;
    }
  }
  return true;
}

bool DocListDeltaBuffer::AddDoc(DocumentID doc_id) {
  if (!doc_list_.empty()) {
    if (doc_id >= last_docid_) {
      SearchLogError(
          "DeltaBuffer::AddDoc fatal error, last_docid=%llu, doc_id=%llu",
          (uint64_t)last_docid_, (uint64_t)doc_id);
      return false;
    }
  }
  this->doc_list_.push_back(doc_id);
  last_docid_ = doc_id;
  return true;
}

bool DocListDeltaBuffer::SerializeToString(std::string &buffer) {
  DocumentID prev_doc_id = 0;
  for (const auto &doc_id : doc_list_) {
    if (prev_doc_id == 0)
      PutVarint64(&buffer, doc_id);
    else
      PutVarint64(&buffer, prev_doc_id - doc_id);
    prev_doc_id = doc_id;
  }
  return true;
}

// Must keep doc_id in order,otherwise will return false;
bool DocListCompressionEncoder::AddDoc(DocumentID doc_id, bool is_del) {
  assert(true == delta_buffer.AddDoc(doc_id));
  if (is_del) {
    assert(true == del_buffer.AddDeletePos(pos_));
    has_del_ = true;
  }
  pos_++;
  return true;
}

bool DocListCompressionEncoder::SerializeToString(std::string &buffer) {
  if (has_del_) {
    DocListCompressionVarLenBlockFlag flag;
    flag.SetHasDelete();
    header.flag = flag.Value();
  }
  buffer.assign((const char *)&header, sizeof(header));

  if (has_del_) {
    assert(true == this->del_buffer.SerializeToString(buffer));
  }

  assert(true == this->delta_buffer.SerializeToString(buffer));

  return true;
}

bool DocListCompressionDecoder::Decode(const std::string &buffer,
                                       std::string &fix_doclist) {
  return this->Decode(buffer.c_str(), buffer.size(), fix_doclist);
}

bool DocListCompressionDecoder::Decode(const char *ptr, size_t len,
                                       std::string &fix_doclist) {
#define CheckRet(ret) \
  if (!ret) return false

  Slice slice(ptr, len);

  // decode header
  if (slice.size() < sizeof(DocListHeader)) return false;
  DocListHeader header = *(DocListHeader *)slice.data();
  slice.remove_prefix(sizeof(DocListHeader));
  if (header.version != DocListCompressionVarLenBlockType) return false;
  fix_doclist.append((const char *)&header, sizeof(DocListHeader));

  // decode del buffer
  DocListCompressionVarLenBlockFlag flag(header.flag);
  std::vector<uint32_t> del_position;
  if (flag.HasDelete()) {
    uint32_t del_num;
    uint32_t prev_pos = 0, pos;
    CheckRet(GetVarint32(&slice, &del_num));
    for (uint32_t i = 0; i < del_num; i++) {
      CheckRet(GetVarint32(&slice, &pos));
      del_position.push_back(prev_pos + pos);
      prev_pos += pos;
    }
  }

  // decode doc id
  size_t del_idx = 0;
  DocumentID prev_doc_id = 0;
  size_t doc_num = 0;
  for (;;) {
    DocumentID doc_id;
    bool success = GetVarint64(&slice, &doc_id);  // only store delta
    // reach end
    if (!success) break;
    if (0 != prev_doc_id) {
      doc_id = prev_doc_id - doc_id;
    }
    prev_doc_id = doc_id;
    fix_doclist.append((const char *)&doc_id, sizeof(DocumentID));

    DocumentState state = 0;
    // del_position in increase order
    if (del_idx < del_position.size()) {
      if (del_position[del_idx] == doc_num) {
        state = 1;
        del_idx++;  // next one position must > doc_num
      } else if (del_position[del_idx] < doc_num) {
        assert(false);
      }
    }
    fix_doclist.append((const char *)&state, sizeof(DocumentState));
    doc_num++;
  }

  return true;
}

}  // namespace wwsearch
