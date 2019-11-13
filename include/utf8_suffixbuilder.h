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
#include "header.h"

namespace wwsearch {
class UTF8SuffixBuilder {
 private:
  const char *buffer_;
  char *it;
  size_t buffer_size_;
  size_t pos_;
  bool error;
  uint32_t min_suffix_len_;

 public:
  // make sure,buffer is valid utf-8 string
  UTF8SuffixBuilder(const char *buffer, size_t buffer_size,
                    uint32_t min_suffix_len)
      : buffer_(buffer),
        it((char *)buffer),
        buffer_size_(buffer_size),
        pos_(0),
        error(false),
        min_suffix_len_(min_suffix_len) {
    // fix min len
    if (min_suffix_len > buffer_size_) {
      min_suffix_len_ = buffer_size_;
    }
  }

  virtual ~UTF8SuffixBuilder() {}

  // return false,if reach end.
  bool Next();

  const char *Term();

  size_t TermSize();

  inline void Reset();

 private:
};
}  // namespace wwsearch
