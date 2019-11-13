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

#include "codec.h"
#include "scorer.h"

namespace wwsearch {

class BooleanScorer : public Scorer {
 private:
  DocIdSetIterator *iterator_;
  Codec *codec_;

 public:
  BooleanScorer(Weight *weight, DocIdSetIterator *iterator, Codec *codec)
      : Scorer(weight, "BooleanScorer"), iterator_(iterator), codec_(codec) {}

  virtual ~BooleanScorer();

  virtual DocumentID DocID();

  virtual double Score();

  virtual DocIdSetIterator &Iterator();

 private:
};

}  // namespace wwsearch
