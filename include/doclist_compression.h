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
#include "codec_doclist.h"
#include "coding.h"
#include "document.h"
#include "header.h"

namespace wwsearch {

enum DocListCompressionType {
  DocListCompressionFixType = 0,
  // Format:
  // [header][delete flag block][doc list block]
  DocListCompressionVarLenBlockType = 1,
};

struct DocListCompressionVarLenBlockFlag_t {
  uint8_t flag_ : 3;
  DocListCompressionVarLenBlockFlag_t() : flag_(0) {}
  DocListCompressionVarLenBlockFlag_t(uint8_t flag) : flag_(flag) {}
  void SetHasDelete() { this->flag_ = 1; }
  bool HasDelete() { return flag_; }
  uint8_t Value() { return this->flag_; }
} __attribute__((packed));

typedef struct DocListCompressionVarLenBlockFlag_t
    DocListCompressionVarLenBlockFlag;

class DocListDelDeltaBuffer {
 private:
  uint32_t del_num_;
  uint32_t last_pos_;
  std::vector<uint32_t> del_position_;

 public:
  DocListDelDeltaBuffer() {
    del_num_ = 0;
    last_pos_ = 0;
  }
  virtual ~DocListDelDeltaBuffer() {}

  bool AddDeletePos(size_t pos);

  virtual bool SerializeToString(std::string &buffer);

 private:
};

class DocListDeltaBuffer {
 private:
  DocumentID last_docid_;
  std::vector<DocumentID> doc_list_;

 public:
  DocListDeltaBuffer() {}
  virtual ~DocListDeltaBuffer() {}

  bool AddDoc(DocumentID doc_id);

  virtual bool SerializeToString(std::string &buffer);

 private:
};

class DocListCompressionEncoder {
 private:
  DocListHeader header;
  DocListDelDeltaBuffer del_buffer;
  DocListDeltaBuffer delta_buffer;
  size_t pos_;
  bool has_del_;

 public:
  DocListCompressionEncoder() {
    header.version = DocListCompressionVarLenBlockType;
    pos_ = 0;
    has_del_ = false;
  }

  virtual ~DocListCompressionEncoder() {}

  // Must keep doc_id in order,otherwise will return false;
  virtual bool AddDoc(DocumentID doc_id, bool is_del = false);

  virtual bool SerializeToString(std::string &buffer);

 private:
};

class DocListCompressionDecoder {
 private:
 public:
  virtual bool Decode(const std::string &buffer, std::string &fix_doclist);
  virtual bool Decode(const char *ptr, size_t len, std::string &fix_doclist);

 private:
};
}  // namespace wwsearch