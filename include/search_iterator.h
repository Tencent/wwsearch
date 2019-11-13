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

#include "search_slice.h"
#include "search_status.h"

// wrapper interface
namespace wwsearch {

class Iterator {
 public:
  Iterator() {}
  virtual ~Iterator() {}

  virtual bool Valid() const = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual void Seek(const Slice& target) = 0;

  virtual void SeekForPrev(const Slice& target) = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  virtual Slice key() const = 0;

  virtual Slice value() const = 0;

  virtual SearchStatus status() const = 0;
};
}  // namespace wwsearch
