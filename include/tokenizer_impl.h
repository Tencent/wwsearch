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

#include "tokenizer.h"

namespace wwsearch {

class TokenizerSpaceImpl : public Tokenizer {
 private:
 public:
  TokenizerSpaceImpl() {}
  virtual ~TokenizerSpaceImpl() {}

  virtual bool Do(wwsearch::Document &document) override;

  virtual bool BuildTerms(const char *buffer, size_t buffer_size,
                          std::set<std::string> &terms,
                          bool no_covert_to_lower_case = false) override;

  inline bool IsSkipChar(char c) { return c == ' ' || c == '\t' || c == '\r'; }

  virtual void ToLowerCase(std::string &old) override {}

 private:
};
}  // namespace wwsearch
