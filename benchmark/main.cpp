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

#include <stdlib.h>
#include "include/search_util.h"

#include "bench_db.h"
#include "bench_index.h"
#include "bench_merge.h"
#include "bench_random.h"

bool g_use_rocksdb = false;

typedef void (*CasePtr)(wwsearch::ArgsHelper &args);
typedef struct Case {
  CasePtr handler;
  std::string description;
  std::string usage;
} Case;

// do not use -c param.
Case cases[] = {
    {.handler = wwsearch::BenchIndex::Run,
     .description = wwsearch::BenchIndex::Description,
     .usage = wwsearch::BenchIndex::Usage},
    {.handler = wwsearch::BenchMerge::Run,
     .description = wwsearch::BenchMerge::Description,
     .usage = wwsearch::BenchMerge::Usage},
    {.handler = wwsearch::BenchRandom::Run,
     .description = wwsearch::BenchRandom::Description,
     .usage = wwsearch::BenchRandom::Usage},
    {.handler = wwsearch::BenchDB::Run,
     .description = wwsearch::BenchDB::Description,
     .usage = wwsearch::BenchDB::Usage},
};

void ShowUsage(char **argv) {
  printf("Usage:\n");
  for (int i = 0; i < sizeof(cases) / sizeof(Case); i++) {
    printf("\t#%s \n\t -c %d %s\n\n", cases[i].description.c_str(), i + 1,
           cases[i].usage.c_str());
  }
}

int main(int argc, char **argv) {
  wwsearch::ArgsHelper helper(
      argc, argv, "a:d:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:");

  int case_idx = helper.Int('c');
  if (helper.Int('r') == 1) {
    g_use_rocksdb = true;
  }
  if (sizeof(cases) / sizeof(Case) < case_idx || case_idx <= 0) {
    ShowUsage(argv);
    return -1;
  }

  cases[case_idx - 1].handler(helper);
  return 0;
}
