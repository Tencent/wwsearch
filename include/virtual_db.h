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
#include "logger.h"
#include "rocksdb/options.h"
#include "search_iterator.h"
#include "storage_type.h"
#include "virtual_db_rocks_compaction_filter.h"
#include "write_buffer.h"

namespace wwsearch {

class IndexConfig;
typedef struct VDBParams {
  std::string path;
  // we must have two file in mmseg_dict_dir_
  // * uni.lib
  // * mmseg.ini
  std::string mmseg_dict_dir_ = "./";
  uint64_t mmseg_instance_num_ = 20;
  // paxos log expire time (seconds)
  // <=0 indicate infinite
  int32_t plog_ttl_seconds_ = 86400;
  // rocksdb
  uint32_t rocks_max_background_jobs_{8};
  uint32_t rocks_max_subcompactions{1};
  uint64_t rocks_max_bytes_for_level_base = 256 << 20; /* 256MB */

  bool rocks_allow_concurrent_memtable_write{true};
  int32_t rocks_num_levels{7};
  int32_t rocks_level0_file_num_compaction_trigger{4};
  int32_t rocks_level0_slowdown_writes_trigger{20};
  int32_t rocks_level0_stop_writes_trigger{36};

  // reference to index config
  // IndexConfig* config_;

  // rocksdb
  uint64_t rocks_max_log_file_size = 50 << 20;  // 50MB
  uint64_t rocks_log_file_time_to_roll = 3600;  // one hours
  uint64_t rocks_keep_log_file_num = 240;       // 10 days

  // manifest
  uint64_t rocks_max_manifest_file_size = 50 << 20;  // 50MB

  // write
  uint64_t rocks_write_buffer_size = 64 << 20;      // 64 MB
  uint64_t rocks_max_write_buffer_number = 6;       // 6 write buffer
  uint64_t rocks_max_total_wal_size = 100 << 20;    // 200 MB
  uint64_t rocks_db_write_buffer_size = 100 << 20;  // 200 MB
  // uint64_t rocks_wal_bytes_per_sync = 20 << 20; // 2MB sync once ?
  // uint64_t rocks_bytes_per_sync = 20 << 20;

  // table cache block cache
  uint64_t rocks_db_block_cache_bytes = 0;  // in bytes. if 0,not set
  bool rocks_db_cache_index_and_filter_blocks = false;
  uint64_t rocks_db_max_open_files = 0;  // if 0 ,not set

  // compression
  rocksdb::CompressionType rocks_compression = rocksdb::kSnappyCompression;

  std::map<StorageColumnType, VirtualDBRocksCompactionFilter*>
      columns_compactionfilter;

  Codec* codec_;
  uint32_t max_doc_list_num_ = 1000000;
} VDBParams;

class VirtualDBSnapshot {
 private:
 public:
  virtual ~VirtualDBSnapshot() {}

 private:
};

class VirtualDBWriteOption {
 private:
 public:
  virtual ~VirtualDBWriteOption() {}

 private:
};

class VirtualDBReadOption {
 public:
  VirtualDBSnapshot* snapshot_;

 public:
  VirtualDBReadOption() : snapshot_(nullptr) {}

  virtual ~VirtualDBReadOption() {}

 private:
};

class VirtualDB {
 private:
  std::string msg_if_error;

 public:
  virtual ~VirtualDB(){};

  virtual bool Open() = 0;

  // snapshot
  virtual VirtualDBSnapshot* NewSnapshot() = 0;

  virtual void ReleaseSnapshot(VirtualDBSnapshot*) = 0;

  // for write
  virtual SearchStatus FlushBuffer(WriteBuffer* write_buffer) = 0;

  // virtual SearchStatus FlushBuffer(const std::string* write_buffer) = 0;

  virtual WriteBuffer* NewWriteBuffer(const std::string* write_buffer) = 0;

  virtual void ReleaseWriteBuffer(WriteBuffer* buffer) = 0;

  // for read
  virtual SearchStatus Get(StorageColumnType column, const std::string& key,
                           std::string& value, VirtualDBSnapshot* snapshot) = 0;

  // columns's size and keys's size should be equal.
  // status and values 's size will alway resize to keys's size.
  virtual void MultiGet(std::vector<StorageColumnType> columns,
                        std::vector<std::string>& keys,
                        std::vector<std::string>& values,
                        std::vector<SearchStatus>& status,
                        VirtualDBSnapshot* snapshot) = 0;

  // for iterator
  virtual Iterator* NewIterator(StorageColumnType column,
                                VirtualDBReadOption* options) = 0;

  // Compact the underlying storage for the key range [*begin,*end]
  virtual SearchStatus CompactRange(StorageColumnType column,
                                    const std::string& begin,
                                    const std::string& end) = 0;

  inline void SetState(const char* msg) { this->msg_if_error.assign(msg); }

  inline std::string& GetState() { return this->msg_if_error; }

  // for drop

  virtual SearchStatus DropDB() = 0;

 private:
};
}  // namespace wwsearch
