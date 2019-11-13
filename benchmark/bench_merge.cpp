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

#include "include/header.h"

#include "bench_merge.h"
#include "random_creater.h"
#include "staticstic.h"

namespace wwsearch {

// merge performance
// * orginal implementation
// * personal implementation
//
// Some tips:
// * pre allocation array for heap
// * reserve string size previous,or use buffer
//	 1000,0000 -> 50%
// * implement heap algorithm
//
const char* BenchMerge::Description =
    "-q [queue_size] -n [doc id num] -r [run times] ";

const char* BenchMerge::Usage = "Benchmark for merge doc list ";

void BenchMerge::Run(wwsearch::ArgsHelper& args) { return; }

bool BenchMerge::CurrDocListMerge(Codec* codec,
                                  const std::deque<std::string>& operand_list,
                                  std::string* new_value, bool purge_deleted) {
  return true;
}

}  // namespace wwsearch
