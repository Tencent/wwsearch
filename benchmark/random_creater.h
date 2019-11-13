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
#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>

namespace wwsearch {

class RandomCreater {
 private:
  uint64_t count_;
  uint64_t seed1_;
  uint64_t seed2_;
  std::string temp_str_;

 public:
  RandomCreater() {}

  virtual ~RandomCreater() {}

  inline int Init(unsigned int seed) {
    int fd;
    fd = open("/dev/urandom", 0);
    if (fd < 0) {
      return -1;
    }

    assert(read(fd, &seed1_, 4) > 0);
    assert(read(fd, &seed2_, 4) > 0);
    close(fd);
    count_ = 0;
    return 0;
  }

  inline uint32_t GetUInt32() {
    count_++;
    if (count_ % 2 == 0) {
      seed1_ = (seed1_ * 25214903917UL + 11UL) % (1UL << 48);
      return seed1_;
    } else {
      seed2_ = (seed2_ * 25214903917UL + 11UL) % (1UL << 48);
      return seed2_;
    }
  }

  inline uint64_t GetUInt64() {
    uint64_t v;
    v = GetUInt32();
    v <<= 32;
    v |= GetUInt32();
    return v;
  }

  std::string &GetString(size_t len) {
    temp_str_.clear();
    uint32_t v;

    while (len > 4) {
      v = GetUInt32();
      temp_str_.append((const char *)&v, 4);
      len -= 4;
    }
    v = GetUInt32();
    temp_str_.append((const char *)&v, len);

    return temp_str_;
  }
};

}  // namespace wwsearch
