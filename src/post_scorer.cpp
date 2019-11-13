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

#include "post_scorer.h"
#include "logger.h"

namespace wwsearch {
std::shared_ptr<ScoreStrategy> ScoreStrategyFactory::NewScoreStrategy(
    const std::string &pattern, size_t terms_size, FieldID target_field_id) {
  return std::make_shared<StringScoreStrategy>(pattern, terms_size,
                                               target_field_id);
}

DocumentScore StringScoreStrategy::Score(Document *document,
                                         int match_field_id) {
  DocumentScore score{0.0};
  if (target_field_id_ != static_cast<FieldID>(match_field_id)) {
    return score;
  }
  IndexField *index_field = document->FindField(match_field_id);
  if (index_field->FieldType() != kStringIndexField) {
    return score;
  }
  if (index_field->StringValue().find(pattern_) != std::string::npos) {
    SearchLogDebug("find pattern{%s}, index string{%s} index_field addr {%p}",
                   pattern_.c_str(), index_field->StringValue().c_str(),
                   index_field);
    score += weight_;
  }
  if (index_field->Terms().size() > 0) {
    double terms_score = (terms_size_ * 1.0) / index_field->Terms().size();
    score += weight_ * terms_score;
    SearchLogDebug("terms_score {%f}, total score {%f}", terms_score, score);
  }
  return score;
}

}  // namespace wwsearch