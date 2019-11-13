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
namespace wwsearch {

enum kSearchStatusCode {
  kOK = 0,
  kSearchStatusCodeBase = -18525,
  kDocumentExistStatus = 18526,
  kDocumentNotExistStatus = 18527,
  kSerializeErrorStatus = -18528,
  kRocksDBErrorStatus = -18529,
  kDocumentIDCanNotZero = 18530,
  kDataErrorStatus = -18531,
  kTokenizerErrorStatus = -18532,
  kOtherDocumentErrorStatus = 18533,
  kIndexWriterOpenErrorStatus = -18534,
  kVirtualDBOpenErrorStatus = -18535,
  kScorerErrorStatus = -18536,
  kDocumentTooLargeStatus = 18537,
  kReachMaxDocListSizeLimit = 18538,
};

class SearchStatus {
 private:
  int status_code_;
  std::string status_msg_;

 public:
  SearchStatus();

  SearchStatus(const SearchStatus &o);

  ~SearchStatus();

  void SetStatus(int status_code, const char *msg);

  const std::string &GetState() const;

  bool DocumentExist();

  bool DocumentNotExist();

  bool ReachMaxDocListSizeLimit();

  bool OK();

  int GetCode() const;

 private:
};
}  // namespace wwsearch
