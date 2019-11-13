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

#include "scorer.h"
#include "storage_type.h"

namespace wwsearch {

/* Notice : Core class for collect & filter & sort the suitable doc list.
 */
class Collector {
 private:
  Scorer *scorer_;

 protected:
  SearchStatus status_;

 public:
  Collector() : scorer_(nullptr) {}

  virtual ~Collector() {}

  inline void SetScorer(Scorer *scorer) { this->scorer_ = scorer; }

  inline Scorer *GetScorer() { return this->scorer_; }

  virtual void Collect(DocumentID doc, int field_id) = 0;

  virtual bool Enough() = 0;

  virtual void Finish() = 0;

  // Do not call this method more than once.
  virtual void GetAndClearMatchDocs(std::list<DocumentID> &docs) = 0;

  inline SearchStatus &Status() { return this->status_; }

 private:
};

}  // namespace wwsearch
