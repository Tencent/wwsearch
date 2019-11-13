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

#define DELETE_RESET(ptr) \
  if ((ptr) != nullptr) { \
    delete (ptr);         \
    (ptr) = nullptr;      \
  }

class ArgsHelper {
 private:
  std::map<char, std::string> _kv;

 public:
  // usage: man 3 getopt
  // example:   a:b:cd
  ArgsHelper(int argc, char **argv, const char *opts_str);
  ~ArgsHelper();

  bool Have(const char c);

  std::string String(const char c);

  int Int(const char c);

  int64_t Int64(const char c);

  uint32_t UInt(const char c);

  uint64_t UInt64(const char c);
};
}  // namespace wwsearch
