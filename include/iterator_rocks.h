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

#include "rocksdb/iterator.h"
#include "search_iterator.h"

namespace wwsearch {

/* Need only in using rocksdb.
 * A wrapper for rocksdb interator to return SearchStatus.
 */
class IteratorRocks : public Iterator {
 private:
  rocksdb::Iterator* iterator_;

 public:
  IteratorRocks(rocksdb::Iterator* iterator) : iterator_(iterator) {}

  virtual ~IteratorRocks() {
    if (iterator_ != nullptr) {
      delete iterator_;
      iterator_ = nullptr;
    }
  }

  virtual bool Valid() const { return this->iterator_->Valid(); }

  virtual void SeekToFirst() { this->iterator_->SeekToFirst(); }

  virtual void SeekToLast() { this->iterator_->SeekToLast(); }

  virtual void Seek(const Slice& target) {
    rocksdb::Slice slice_target(target.data(), target.size());
    this->iterator_->Seek(slice_target);
  }

  virtual void SeekForPrev(const Slice& target) {
    rocksdb::Slice slice_target(target.data(), target.size());
    this->iterator_->SeekForPrev(slice_target);
  }

  virtual void Next() { this->iterator_->Next(); }

  virtual void Prev() { this->iterator_->Prev(); }

  virtual Slice key() const {
    auto rocks_slice = this->iterator_->key();
    return wwsearch::Slice(rocks_slice.data(), rocks_slice.size());
  }

  virtual Slice value() const {
    auto rocks_slice = this->iterator_->value();
    return wwsearch::Slice(rocks_slice.data(), rocks_slice.size());
  }

  virtual SearchStatus status() const {
    auto status = this->iterator_->status();
    wwsearch::SearchStatus ss;

    if (!status.ok()) {
      ss.SetStatus(kRocksDBErrorStatus, status.getState());
    }
    return ss;
  }

 private:
};
}  // namespace wwsearch
