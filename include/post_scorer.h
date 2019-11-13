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

#include <memory>
#include "document.h"
#include "index_field.h"

namespace wwsearch {

class ScoreStrategy {
 public:
  virtual DocumentScore Score(Document *document, int match_field_id) = 0;
};

class ScoreStrategyFactory {
 public:
  static std::shared_ptr<ScoreStrategy> NewScoreStrategy(
      const std::string &pattern, size_t terms_size, FieldID target_field_id);
};

class StringScoreStrategy : public ScoreStrategy {
 private:
  std::string pattern_;
  size_t terms_size_;
  FieldID target_field_id_;
  size_t weight_{10};

 public:
  StringScoreStrategy(const std::string &pattern, size_t terms_size,
                      FieldID target_field_id)
      : pattern_(pattern),
        terms_size_(terms_size),
        target_field_id_(target_field_id) {}
  virtual ~StringScoreStrategy() {}
  virtual DocumentScore Score(Document *document, int match_field_id) override;
};

class PostScorer {
 private:
  // std::vector<std::shared_ptr<ScoreStrategy>> *score_strategy_list_;

 public:
  PostScorer() {}
  ~PostScorer() {}
  inline bool operator()(Document *lhs, Document *rhs) const {
    if (lhs->Score() > rhs->Score()) {
      return false;
    } else {
      return true;
    }
  }
};
}  // namespace wwsearch