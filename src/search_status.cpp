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

#include "search_status.h"

namespace wwsearch {

SearchStatus::SearchStatus() : status_code_(0) {}

SearchStatus::SearchStatus(const SearchStatus &o) {
  this->status_code_ = o.GetCode();
  this->status_msg_ = o.GetState();
}

SearchStatus::~SearchStatus() {}

void SearchStatus::SetStatus(int status_code, const char *msg) {
  status_code_ = status_code;
  status_msg_.assign(msg);
}

const std::string &SearchStatus::GetState() const { return this->status_msg_; }

bool SearchStatus::DocumentExist() {
  return this->status_code_ == kDocumentExistStatus;
}

bool SearchStatus::DocumentNotExist() {
  return this->status_code_ == kDocumentNotExistStatus;
}

bool SearchStatus::ReachMaxDocListSizeLimit() {
  return this->status_code_ == kReachMaxDocListSizeLimit;
}

bool SearchStatus::OK() { return this->status_code_ == 0; }

int SearchStatus::GetCode() const { return this->status_code_; }

}  // namespace wwsearch
