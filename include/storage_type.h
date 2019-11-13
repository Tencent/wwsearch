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
#include <string>

namespace wwsearch {

// Document id
typedef uint64_t DocumentID;

// Cost
typedef uint32_t CostType;

//  Score
typedef uint32_t ScoreType;

// Field id
typedef uint8_t FieldID;

#define MaxFieldID 255;

typedef double DocumentScore;

typedef struct TableID {
  uint8_t business_type;
  uint64_t partition_set;

  std::string PrintToStr() const {
    char buffer[20];
    snprintf(buffer, 20, "%u:%llu", business_type, partition_set);
    return std::string(buffer);
  }
} TableID;

typedef uint8_t FieldType;

typedef enum StorageColumnType {
  // WARNING:
  // DO NOT CHANGE ORDER
  // IF ADD NEW COLUMN,MUST CHECK CERTIAN'S GETALL ROUTINE.
  kStoredFieldColumn = 0,    // store document
  kInvertedIndexColumn = 1,  // store invert doc list of match term
  kDocValueColumn = 2,       // store table doc value of every document
  kMetaColumn = 3,           // store user'id mapping currently
  kDictionaryColumn = 4,     // store nothing

  // NOTICE:
  // BELOW COLUMN WILL NOT USE FOR CERTAIN'S GETALL ROUTINE.
  kPaxosLogColumn = 5,
  kPaxosDataMetaColumn = 6,
  kPaxosLogMetaColumn = 7,

  // MAX
  kMaxColumn = 8
} StorageColumnType;
}  // namespace wwsearch
