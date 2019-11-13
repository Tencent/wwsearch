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

class BenchDB {
 private:
 public:
  BenchDB() {}

  virtual ~BenchDB() {}

  static const char *Usage;

  static const char *Description;

  static void Run(wwsearch::ArgsHelper &args);

  static void ThreadRun(DefaultIndexWrapper &wrapper, uint64_t run_times,
                        Staticstic &staticstic, RandomCreater *random_creater,
                        int mode);

  static void PrintStaitic(Staticstic &statistic);

 private:
};
}  // namespace wwsearch