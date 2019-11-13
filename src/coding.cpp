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

#include "coding.h"

namespace wwsearch {
namespace internal {
// encoding 1 byte to key
void AppendFixed8(std::string& key, uint8_t i) { key.push_back(i); }

// encoding 8 byte(big endian order) to key
void AppendFixed64(std::string& key, uint64_t i) {
  key.append((char*)&i, sizeof(i));
}

// encoding buffer byte to key
void AppendBuffer(std::string& key, const char* buffer, uint32_t size) {
  key.append(buffer, size);
}

// remove 1 bytes to value<i>
void RemoveFixed8(Slice& key, uint8_t& i) {
  i = *(uint8_t*)key.data();
  key.remove_prefix(sizeof(i));
}

// remove 8 bytes to value <i>
void RemoveFixed64(Slice& key, uint64_t& i) { key.remove_prefix(sizeof(i)); }

// remove buffer
void RemoveBuffer(Slice& key, char* buffer, uint32_t size) {
  memcpy(buffer, key.data(), size);
  key.remove_prefix(size);
}
}  // namespace internal

//  varlen encoding of uint32
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1 << 7)) {
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }
  return reinterpret_cast<char*>(ptr);
}

// get value from valen encoding bytes.
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

// get value from varlen encoding bytes.
const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

// big endian order to host order
uint64_t ntohll(uint64_t i) {
  if (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return (((uint64_t)ntohl((uint32_t)i)) << 32) |
           (uint64_t)(ntohl((uint32_t)(i >> 32)));
  }
  return i;
}

// host order to big endian order
uint64_t htonll(uint64_t i) {
  if (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return (((uint64_t)htonl((uint32_t)i)) << 32) |
           ((uint64_t)htonl((uint32_t)(i >> 32)));
  }
  return i;
}

// append one byte
void AppendFixed8(std::string& key, uint8_t i) { key.push_back(i); }

// append 2 bytes with big endian order
void AppendFixed16(std::string& key, uint16_t i) {
  i = htons(i);
  key.append((char*)&i, sizeof(i));
}
// append 4 bytes with big endian order
void AppendFixed32(std::string& key, uint32_t i) {
  i = htonl(i);
  key.append((char*)&i, sizeof(i));
}

// append 8 bytes with big endian order
void AppendFixed64(std::string& key, uint64_t i) {
  i = htonll(i);
  key.append((char*)&i, sizeof(i));
}

// append buffer
void AppendBuffer(std::string& key, const char* buffer, uint32_t size) {
  key.append(buffer, size);
}

// remove 1 byte
void RemoveFixed8(Slice& key, uint8_t& i) {
  i = *(uint8_t*)key.data();
  key.remove_prefix(sizeof(i));
}

// remove 2 byte
void RemoveFixed16(Slice& key, uint16_t& i) {
  i = ntohs(*(uint16_t*)key.data());
  key.remove_prefix(sizeof(i));
}

// remove 4 byte
void RemoveFixed32(Slice& key, uint32_t& i) {
  i = ntohl(*(uint32_t*)key.data());
  key.remove_prefix(sizeof(i));
}

// remove 8 byte
void RemoveFixed64(Slice& key, uint64_t& i) {
  i = ntohll(*(uint64_t*)key.data());
  key.remove_prefix(sizeof(i));
}

// remove some buffer
void RemoveBuffer(Slice& key, char* buffer, uint32_t size) {
  memcpy(buffer, key.data(), size);
  key.remove_prefix(size);
}

// get varlen buffer's address
const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

// encode varlen uint64
char* EncodeVarint64(char* dst, uint64_t v) {
  static const unsigned int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

// encode varlen uint32
void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

// encode 2 varlen uint32
void PutVarint32Varint32(std::string* dst, uint32_t v1, uint32_t v2) {
  char buf[10];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}
// encode 3 varlen uint32
void PutVarint32Varint32Varint32(std::string* dst, uint32_t v1, uint32_t v2,
                                 uint32_t v3) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint32(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

// put varlen uint64
void PutVarint64(std::string* dst, uint64_t v) {
  char buf[MAXVARINT64LENGTH];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

// put varlen uint64 + varlen uint64
void PutVarint64Varint64(std::string* dst, uint64_t v1, uint64_t v2) {
  char buf[20];
  char* ptr = EncodeVarint64(buf, v1);
  ptr = EncodeVarint64(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

// put varlen uint32 + varllen uint64
void PutVarint32Varint64(std::string* dst, uint32_t v1, uint64_t v2) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint64(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

// put varlen uint32 + varlen uint32 + varlen uint64
void PutVarint32Varint32Varint64(std::string* dst, uint32_t v1, uint32_t v2,
                                 uint64_t v3) {
  char buf[20];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint64(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

// get one varlen uint32
bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

// get one varlen uint64
bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

}  // namespace wwsearch
