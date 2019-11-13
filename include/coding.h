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

#include "header.h"
#include "search_slice.h"

// Borrowed from rocksdb or level db
namespace wwsearch {

/* Notice : This is the set of helper functions. Support for :
 * 1. encode&decode converts uint32/uint64 to string and reverse.
 * 2. Big-end/little-end keep in same.
 * 3. Variable length coding.
 */
#define MAXVARINT64LENGTH 10

namespace internal {
void AppendFixed8(std::string& key, uint8_t i);

void AppendFixed64(std::string& key, uint64_t i);

void AppendBuffer(std::string& key, const char* buffer, uint32_t size);

void RemoveFixed8(Slice& key, uint8_t& i);

void RemoveFixed64(Slice& key, uint64_t& i);

void RemoveBuffer(Slice& key, char* buffer, uint32_t size);

}  // namespace internal

uint64_t ntohll(uint64_t i);

uint64_t htonll(uint64_t i);

void AppendFixed8(std::string& key, uint8_t i);

void AppendFixed16(std::string& key, uint16_t i);

void AppendFixed32(std::string& key, uint32_t i);

void AppendFixed64(std::string& key, uint64_t i);

void AppendBuffer(std::string& key, const char* buffer, uint32_t size);

void RemoveFixed8(Slice& key, uint8_t& i);

void RemoveFixed16(Slice& key, uint16_t& i);

void RemoveFixed32(Slice& key, uint32_t& i);

void RemoveFixed64(Slice& key, uint64_t& i);

void RemoveBuffer(Slice& key, char* buffer, uint32_t size);

const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value);

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value);

const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value);

char* EncodeVarint32(char* dst, uint32_t v);

char* EncodeVarint64(char* dst, uint64_t v);

void PutVarint32(std::string* dst, uint32_t v);

void PutVarint32Varint32(std::string* dst, uint32_t v1, uint32_t v2);

void PutVarint32Varint32Varint32(std::string* dst, uint32_t v1, uint32_t v2,
                                 uint32_t v3);

void PutVarint64(std::string* dst, uint64_t v);

void PutVarint64Varint64(std::string* dst, uint64_t v1, uint64_t v2);

void PutVarint32Varint64(std::string* dst, uint32_t v1, uint64_t v2);

void PutVarint32Varint32Varint64(std::string* dst, uint32_t v1, uint32_t v2,
                                 uint64_t v3);

bool GetVarint32(Slice* input, uint32_t* value);

bool GetVarint64(Slice* input, uint64_t* value);

}  // namespace wwsearch