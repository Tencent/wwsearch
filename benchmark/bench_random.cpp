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

#include "bench_random.h"
#include "random_creater.h"
#include "staticstic.h"

namespace wwsearch {

// Test random ?

const char* BenchRandom::Description =
    "-n [create id num] -m [0:32 1:64 2:string] -d [debug]";

const char* BenchRandom::Usage = "Benchmark for random";

void BenchRandom::Run(wwsearch::ArgsHelper& args) {
  std::vector<std::set<uint64_t> > containers;
  containers.resize(1000);
  std::set<std::string> containers_str;

  uint64_t conflict_count = 0;
  uint64_t num = args.UInt('n');
  uint64_t left_num = num;
  uint32_t mode = args.UInt('m');
  uint32_t debug = args.UInt('d');

  RandomCreater randomer;
  randomer.Init(time(NULL));

  while (left_num-- > 0) {
    uint32_t temp;
    uint64_t v;
    std::string str;

    if (mode != 2) {
      if (mode == 0) {
        v = randomer.GetUInt32();
      } else if (mode == 1) {
        v = randomer.GetUInt64();
      }

      uint32_t idx = v % 1000;
      auto ret = containers[idx].insert(v);
      if (!ret.second) {
        conflict_count++;
      }
    } else {
      str = randomer.GetString(10);
      auto ret = containers_str.insert(str);
      if (!ret.second) {
        conflict_count++;
      }
    }
    if (debug) {
      if (mode != 2)
        printf("v=%llu\n", v);
      else
        printf("str=%s\n", str.c_str());
    }
  }

  printf("Insert num:%llu,conflict:%llu\n", num, conflict_count);
  return;
}

}  // namespace wwsearch
