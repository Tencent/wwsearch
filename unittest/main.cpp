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

#include <gtest/gtest.h>
#include "include/search_util.h"

bool g_debug = false;
bool g_use_rocksdb = false;
bool g_use_compression = false;

int main(int argc, char **argv) {
  wwsearch::ArgsHelper helper(
      argc, argv, "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:");

  if (1 == helper.Int('r')) {
    g_use_rocksdb = true;
  }
  if (1 == helper.Int('d')) {
    printf("open debug \n ");
    g_debug = true;
  }
  if (1 == helper.Int('p')) {
    g_use_compression = true;
  }
  testing::InitGoogleTest(&argc, argv);
  auto ret = RUN_ALL_TESTS();
  return ret;
}
