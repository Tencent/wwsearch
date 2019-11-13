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
#include "include/index_wrapper.h"
#include "include/search_util.h"
#include "include/staticstic.h"
#include "random_creater.h"

extern bool g_use_rocksdb;

namespace wwsearch {

typedef struct BenchIndexParams {
  uint64_t debug_;
  uint64_t run_times_;
  uint64_t batch_num_;
  uint64_t str_len_;
  uint64_t nummeric_attr_num_;
  uint64_t string_attr_num_;
  uint64_t suffix_attr_num_;
  uint64_t max_uin_num_;
  uint64_t index_type;
  uint64_t perf_rocks;
  uint64_t mock;

  void Print() {
    printf(
        "debug:%llu\n"
        "run_times_:%llu\n"
        "batch_num_:%llu\n"
        "str_len_:%llu\n"
        "numeric_attr_num:%llu\n"
        "string_attr_num:%llu\n"
        "suffix_attr_num:%llu\n"
        "max_uin_num:%llu\n"
        "index_type:%llu\n"
        "perf_rocks:%llu\n",
        debug_, run_times_, batch_num_, str_len_, nummeric_attr_num_,
        string_attr_num_, suffix_attr_num_, max_uin_num_, index_type,
        perf_rocks);
  }
} BenchIndexParams;

class BenchIndex {
 private:
 public:
  BenchIndex() {}

  virtual ~BenchIndex() {}

  static const char *Usage;

  static const char *Description;

  static void Run(wwsearch::ArgsHelper &args);

  static void CollectRocksDBPerf(Staticstic *statistic, uint64_t count = 1,
                                 bool quit = false);

  static void ThreadRun(DefaultIndexWrapper &wrapper, Staticstic &statistic,
                        RandomCreater *random_creater,
                        BenchIndexParams &params);

  static void PrintStaitic(Staticstic &statistic);

 private:
};

}  // namespace wwsearch
