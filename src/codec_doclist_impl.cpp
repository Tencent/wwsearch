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

#include "codec_doclist_impl.h"
#include <cmath>
#include <sstream>
#include "codec.h"
#include "logger.h"
#include "utils.h"

namespace wwsearch {

void DocListOrderWriterCodecImpl::AddDocID(const DocumentID& doc_id,
                                           DocumentState state) {
  internal::AppendFixed64(*data_, doc_id);
  internal::AppendFixed8(*data_, state);
}

std::string DocListOrderWriterCodecImpl::DebugString() {
  std::ostringstream o;

  DocListReaderCodecImpl doc_list_reader(data_->c_str(), data_->size());
  while (doc_list_reader.DocID() != DocIdSetIterator::NO_MORE_DOCS) {
    uint64_t doc_id = static_cast<uint64_t>(doc_list_reader.DocID());
    uint8_t doc_state = doc_list_reader.State();
    o << "<doc_id=" << doc_id << ",doc_state=" << unsigned(doc_state) << ">,";
    doc_list_reader.NextDoc();
  }
  return o.str();
}

bool DocListOrderWriterCodecImpl::SerializeToBytes(std::string& buffer,
                                                   int mode) {
  SearchLogDebug("SerializeToBytes mode=%d, buffer size=%d", mode,
                 this->data_->size());
  data_->swap(buffer);
  return true;
}

bool DocListOrderWriterCodecImpl::DeSerializeFromByte(const char* buffer,
                                                      uint32_t buffer_len) {
  assert(false);
  return false;
}

void CompressionWriterCodecImpl::AddDocID(const DocumentID& doc_id,
                                          DocumentState state) {
  assert(true == encoder_.AddDoc(doc_id, state));
}

bool CompressionWriterCodecImpl::SerializeToBytes(std::string& buffer,
                                                  int mode) {
  bool ret = encoder_.SerializeToString(buffer);
  SearchLogDebug("SerializeToString ret=%d, mode=%d, buffer size=%d", ret, mode,
                 buffer.size());
  return ret;
}

#define DOC_ID_GAP (sizeof(DocumentID) + sizeof(DocumentState))

DocListReaderCodecImpl::DocListReaderCodecImpl(const char* data,
                                               size_t data_len, int field_id)
    : field_id_(field_id) {
  if (0 == data_len) {
    // empty list
    slice_.data_ = data;
    slice_.size_ = data_len;
  } else {
    assert(data_len >= sizeof(DocListHeader));
    DocListHeader header;
    header = *(DocListHeader*)data;

    // In some special case, we only have header without doclist.
    if (header.version == DocListCompressionFixType) {
      SearchLogDebug("decode fix block type,data_len:%u", data_len);
      slice_.data_ = data + sizeof(DocListHeader);
      slice_.size_ = data_len - sizeof(DocListHeader);
    } else if (header.version == DocListCompressionVarLenBlockType) {
      SearchLogDebug("decode compression block type,data_len:%u", data_len);
      DocListCompressionDecoder decoder;
      assert(true == decoder.Decode(data, data_len, buffer_));
      slice_.data_ = buffer_.c_str() + sizeof(DocListHeader);
      slice_.size_ = buffer_.size() - sizeof(DocListHeader);
    } else {
      assert(false);
    }
  }
  pos_ = 0;
}

DocListReaderCodecImpl::~DocListReaderCodecImpl() {}

DocumentID DocListReaderCodecImpl::DocID() {
  if (pos_ >= slice_.size()) return NO_MORE_DOCS;
  DocumentID doc_id = (*(uint64_t*)(slice_.data() + pos_));
  return doc_id;
}

DocumentID DocListReaderCodecImpl::NextDoc() {
  if (NO_MORE_DOCS != DocID()) {
    pos_ += DOC_ID_GAP;
  }
  return DocID();
}

DocumentID DocListReaderCodecImpl::Advance(DocumentID target) {
  if (slice_.size() == 0) return NO_MORE_DOCS;
  if (target == MAX_DOCID) {
    pos_ = 0;
    return DocID();
  }

  // in decrease order
  // find equal or first less than {target} 's document id.
  DocumentID curr_doc_id;
  const char* ptr = slice_.data();
  size_t max_idx = slice_.size() / DOC_ID_GAP;
  size_t min = 0, max = max_idx - 1;
  size_t curr = (min + max) / 2;
  size_t temp;

#define CURR_DOC(idx) (*(DocumentID*)(ptr + DOC_ID_GAP * (idx)))

  while (min <= max) {
    curr_doc_id = CURR_DOC(curr);
    if (curr_doc_id == target) {
      break;
    } else if (curr_doc_id > target) {
      min = curr;
    } else {
      max = curr;
    }

    if (max == min) {
      break;
    } else if (min + 1 == max) {
      if (CURR_DOC(min) > target) {
        curr = max;
      } else if (CURR_DOC(min) < target) {
        curr = min;
      } else {
        curr = min;
      }
      break;
    }
    temp = (min + max) / 2;
    curr = temp;
  }

  pos_ = curr * DOC_ID_GAP;

  // may be we do not have any items less or equal to target. In this case
  // we just set pos_ reach max size so that DOCID() will return NO_MORE_DOC.
  if (DocID() > target) {
    pos_ = this->slice_.size();
  }
  return DocID();
}

CostType DocListReaderCodecImpl::Cost() {
  assert(false);
  return 0;
}

DocumentState& DocListReaderCodecImpl::State() {
  if (pos_ >= slice_.size()) assert(false);
  DocumentState* ptr =
      (DocumentState*)(slice_.data() + pos_ + sizeof(DocumentID));
  return *ptr;
}
}  // namespace wwsearch
