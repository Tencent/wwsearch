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

#include <iterator>
#include <sstream>
#include <string>
#include <vector>

namespace wwsearch {

class Codec;

std::string DebugInvertedValue(const std::string& value);
std::string DebugInvertedValueByReader(Codec* codec, const std::string& value);

void SplitString(const std::string& full, const std::string& delim,
                 std::vector<std::string>* result);
std::string TrimString(const std::string& str, const std::string& trim = " ");

template <class Container>
std::string JoinContainerToString(const Container& c,
                                  const std::string& joiner) {
  std::string result;
  if (!c.empty()) {
    std::ostringstream ss;
    std::copy(c.begin(), c.end(),
              std::ostream_iterator<typename Container::value_type>(
                  ss, joiner.c_str()));
    result = ss.str();
    result.erase(result.length() - 1);
  }
  return result;
}

}  // namespace wwsearch
