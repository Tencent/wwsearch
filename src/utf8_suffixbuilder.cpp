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

#include "utf8_suffixbuilder.h"
#include "checked.h"
#include "logger.h"
#include "unchecked.h"

namespace wwsearch {

// return false,if reach end.
bool UTF8SuffixBuilder::Next() {
  if (error) return error;

  if (true) {
    // utf8
    try {
      utf8::next(it, (char *)(buffer_ + buffer_size_));
      SearchLogDebug("suffix term:%s", it);
    } catch (...) {
      error = true;
      return false;
    }
    pos_ = it - buffer_;
    return pos_ < buffer_size_ && buffer_size_ - pos_ >= min_suffix_len_;
  }

  pos_++;
  return pos_ < buffer_size_ && buffer_size_ - pos_ >= min_suffix_len_;
}

const char *UTF8SuffixBuilder::Term() { return this->buffer_ + pos_; }

size_t UTF8SuffixBuilder::TermSize() {
  assert(pos_ <= this->buffer_size_);
  return this->buffer_size_ - pos_;
}

void UTF8SuffixBuilder::Reset() {
  pos_ = 0;
  error = false;
  it = (char *)buffer_;
}

}  // namespace wwsearch
