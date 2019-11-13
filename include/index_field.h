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

#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <sys/prctl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <sstream>
#include <stack>
#include <string>
#include <vector>

#include "serialize.h"
#include "storage_type.h"

#include "search_store.pb.h"

namespace wwsearch {

/* Notice : Used directly by user to index a document.
 */

enum kIndexFieldType {
  kIndexFieldUnknowType = 0,
  kUint32IndexField = 1,
  kUint64IndexField = 2,
  kStringIndexField = 3,

  kMaxIndexFieldType = 0xFF
};

enum kIndexFieldFlag {
  kTokenizeFieldFlag = 1 << 0,
  kStoreFieldFlag = 1 << 1,
  kDocValueFieldFlag = 1 << 2,
  kSuffixBuildFlag = 1 << 3,
  kInvertIndexFieldFlag = 1 << 4,
  kNotStoreInvertTermFieldFlag = 1 << 5
};

class IndexFieldFlag {
 private:
  unsigned char flag_;

 public:
  IndexFieldFlag() { flag_ = 0; }

  IndexFieldFlag(const IndexFieldFlag& o) { flag_ = o.Flag(); }
  virtual ~IndexFieldFlag();

  void SetTokenize();
  bool Tokenize() const;

  void SetStoredField();
  bool StoredField() const;

  void SetDocValue();
  bool DocValue() const;

  void SetSuffixBuild();
  bool SuffixBuild() const;

  void SetInvertIndex();
  bool InvertIndex() const;

  void SetNotStoreInvertTerm();
  bool NotStoreInvertTerm() const;

  inline unsigned char Flag() const { return this->flag_; }

  inline void SetFlag(unsigned char flag) { this->flag_ = flag; }

 private:
};

// IndexField Construct:
//		field_id,flag,field_type,Length+value or value
class IndexField : public SerializeAble {
  friend class IndexField;

 private:
  FieldID field_id_;
  IndexFieldFlag field_flag_;
  kIndexFieldType field_type_;
  uint64_t numeric_value_;
  std::string string_value_;
  // If we set this to true,wwsearch will not use internal
  // tokenizer to segment text.User must set terms.
  bool use_outer_segment_terms_;  // user have segment terms
  std::set<std::string> terms_;   // in dictionary sorted order
  uint32_t suffix_len_;

  // Only for filter
  // Only work for uint64_t
  std::vector<uint64_t> numeric_list_;

 public:
  IndexField();

  virtual ~IndexField();

  IndexField(const IndexField&) = delete;
  IndexField& operator=(const IndexField&) = delete;

  void CopyFrom(const IndexField& obj);

  inline IndexFieldFlag& Flag() { return this->field_flag_; }

  inline void SetMeta(const FieldID& field_id, const IndexFieldFlag& flag) {
    this->field_id_ = field_id;
    this->field_flag_ = flag;
  }

  void SetUint32(uint32_t value);

  void SetUint64(uint64_t value);

  void SetString(const std::string& value);

  void SetNumericList(const std::vector<uint64_t>& numeric_list);

  const std::vector<uint64_t>& NumericList() const {
    return this->numeric_list_;
  }

  // Attention: be carefully
  void SetSegmentTerms(std::set<std::string>& terms) {
    this->use_outer_segment_terms_ = true;
    this->terms_ = terms;
  }
  bool UseOuterSegmentTerms() { return this->use_outer_segment_terms_; }

  void SetSuffixLen(uint32_t suffix_len);
  uint32_t SuffixLen() const { return this->suffix_len_; }

  virtual bool SerializeToBytes(std::string& buffer, int flag);

  virtual bool DeSerializeFromByte(const char* buffer, uint32_t buffer_len);

  inline std::set<std::string>& Terms() { return this->terms_; }

  inline FieldID ID() const { return this->field_id_; }

  inline const kIndexFieldType& FieldType() const { return this->field_type_; }

  inline const std::string& StringValue() const { return this->string_value_; }

  inline uint64_t NumericValue() const { return this->numeric_value_; }

  // 0->only stored field
  // 1->only doc value
  bool EncodeToStoreField(lsmsearch::StoreIndexField* field, int flag = 0);

  bool DecodeFromStoreField(const lsmsearch::StoreIndexField* field);

  /*
 {
 Document[0]
  - field=0,flag=1,type=2,term_size=3,value=5/str_value=one two three
  - field=0,flag=1,type=2,term_size=3,value=5/str_value=one two three
 }
  */
  void PrintToString(std::string& str) const {
    char* buffer = new char[256];
    snprintf(buffer, 256, "  - field=%u, flag=%d, type=%d, terms_size=%u, ",
             field_id_, field_flag_.Flag(), field_type_, terms_.size());
    str.append(buffer);

    if (field_flag_.StoredField()) {
      if (field_type_ == kStringIndexField) {
        str.append("str_value=").append(string_value_).append("\n");
      } else {
        snprintf(buffer, 256, "value:%llu \n", numeric_value_);
        str.append(buffer);
      }
    } else {
      str.append("\n");
    }
    delete buffer;
  }

 private:
};
}  // namespace wwsearch
