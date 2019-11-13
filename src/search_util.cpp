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

#include "search_util.h"

namespace wwsearch {

ArgsHelper::ArgsHelper(int argc, char **argv, const char *opts_str) {
  int flags, opt;
  int nsecs, tfnd;

  nsecs = 0;
  tfnd = 0;
  flags = 0;
  while ((opt = getopt(argc, argv, opts_str)) != -1) {
    if (NULL != optarg)
      this->_kv[opt] = optarg;
    else
      this->_kv[opt] = "";
  }
}
ArgsHelper::~ArgsHelper() {}

bool ArgsHelper::Have(const char c) {
  return this->_kv.find(c) != this->_kv.end();
}

std::string ArgsHelper::String(const char c) { return this->_kv[c]; }

int ArgsHelper::Int(const char c) { return atoi(this->_kv[c].c_str()); }

int64_t ArgsHelper::Int64(const char c) { return atoll(this->_kv[c].c_str()); }

uint32_t ArgsHelper::UInt(const char c) {
  return strtoul(this->_kv[c].c_str(), NULL, 0);
}

uint64_t ArgsHelper::UInt64(const char c) {
  return strtoull(this->_kv[c].c_str(), NULL, 0);
}
}  // namespace wwsearch
