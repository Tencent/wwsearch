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
#include "header.h"
#include "tokenizer.h"

#ifdef WIN32
#include "bsd_getopt_win.h"
#else
#include "bsd_getopt.h"
#endif

#include "Segmenter.h"
#include "SegmenterManager.h"
#include "SynonymsDict.h"
#include "ThesaurusDict.h"
#include "UnigramCorpusReader.h"
#include "UnigramDict.h"
#include "csr_utils.h"

namespace wwsearch {

class HashLockSupplier {
 public:
  virtual ~HashLockSupplier() {}

  virtual void Lock(uint32_t hash) = 0;

  virtual void unlock(uint32_t hash) = 0;
};

class ThreadHashLock : public HashLockSupplier {
 private:
  std::mutex *locks_;
  uint32_t locks_num_;

 public:
  ThreadHashLock(uint32_t locks_num) {
    this->locks_num_ = locks_num;
    this->locks_ = new std::mutex[locks_num_];
  }
  virtual ~ThreadHashLock() {
    if (nullptr != locks_) {
      delete locks_;
      locks_ = nullptr;
    }
  }

  virtual void Lock(uint32_t hash) { locks_[hash % locks_num_].lock(); }

  virtual void unlock(uint32_t hash) { locks_[hash % locks_num_].unlock(); }

 private:
};

class TokenizerMMSEG : public Tokenizer {
 private:
  std::vector<css::SegmenterManager *> managers_;
  size_t manager_num_;
  HashLockSupplier *hash_lock_;
  std::atomic<std::uint64_t> seq_;

 public:
  // Note:
  // file: dict_path/uni.lib must exist
  // file: dict_path/mmseg.ini must exist
  TokenizerMMSEG(const char *dict_path, size_t segmenter_num = 5000) {
    hash_lock_ = new ThreadHashLock(segmenter_num);
    manager_num_ = segmenter_num;
    assert(manager_num_ > 0);
    for (size_t i = 0; i < manager_num_; i++) {
      css::SegmenterManager *seg = new css::SegmenterManager();
      assert(0 == seg->init(dict_path));
      managers_.push_back(seg);
    }
  }

  virtual ~TokenizerMMSEG() {
    for (auto seg : managers_) delete seg;
    delete hash_lock_;
  }

  virtual bool Do(wwsearch::Document &document) override;

  virtual bool BuildTerms(const char *buffer, size_t buffer_size,
                          std::set<std::string> &terms,
                          bool no_covert_to_lower_case = false) override;

  inline bool IsSkipChar(char c) { return c == ' ' || c == '\t' || c == '\r'; }

  virtual void ToLowerCase(std::string &old) override;

 private:
};
}  // namespace wwsearch

namespace wwsearch {}
