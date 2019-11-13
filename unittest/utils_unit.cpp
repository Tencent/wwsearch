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
#include <stdlib.h>
#include <algorithm>
#include "include/doclist_compression.h"
#include "include/index_wrapper.h"
#include "unittest_util.h"

namespace wwsearch {

class UnitTest : public ::testing::Test {
 public:
 public:
  UnitTest() {}

  static void SetUpTestCase() {}

  static void TearDownTestCase() {}

  virtual void SetUp() override {}

  virtual void TearDown() override {}

 private:
};

int DescSort(const uint64_t &a, const uint64_t &b) { return b < a; }

void GetOrderDocumentIDList(std::vector<uint64_t> &doc_list, size_t n) {
  srandom(time(NULL));
  for (size_t i = 0; i < n; i++) {
    uint64_t doc_id;
    doc_id = (static_cast<uint64_t>(random()) << 32) + random();
    doc_list.push_back(doc_id);
  }
  std::sort(doc_list.begin(), doc_list.end(), DescSort);
}

TEST_F(UnitTest, CompressionDocList) {
  srandom(time(NULL));
  for (size_t i = 0; i < 10; i++) {
    size_t len = random() % 10000 + 1;
    std::vector<uint64_t> doc_list;
    GetOrderDocumentIDList(doc_list, len);
    std::string buffer;
    buffer.reserve(9 * len);

    DocListCompressionEncoder encoder;
    DocListCompressionDecoder decoder;
    for (size_t j = 0; j < len; j++) {
      int delete_flag = random() % 2;
      DocumentID doc_id = doc_list[j];
      buffer.append((const char *)&doc_id, 8);
      DocumentState state = 0;
      if (delete_flag) {
        state = 1;
      }
      buffer.append((const char *)&state, 1);
      ASSERT_TRUE(true == encoder.AddDoc(doc_id, delete_flag));
    }

    std::string buffer1, buffer2;
    ASSERT_TRUE(true == encoder.SerializeToString(buffer1));
    ASSERT_TRUE(true == decoder.Decode(buffer1, buffer2));

    // skip header and do compare
    ASSERT_TRUE(buffer.size() == (buffer2.size() - 1));
    ASSERT_TRUE(0 == memcmp(buffer.c_str(), (char *)(buffer2.c_str() + 1),
                            buffer.size()));
  }
}

}  // namespace wwsearch
