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

#include "virtual_db_mock.h"
#include <iterator>
#include "func_scope_guard.h"
#include "logger.h"
#include "search_status.h"
#include "utils.h"
#include "virtual_db_rocks.h"
#include "write_buffer_mock.h"

namespace wwsearch {

// Constructor
VirtualDBMock::VirtualDBMock(CodecImpl* codec) : codec_(codec) {
  auto do_nothing_func = [](const std::string& key) -> std::string {
    return key;
  };
  cf_debug_string_funcs_.emplace(
      kStoredFieldColumn,
      std::bind(&CodecImpl::DebugStoredFieldKey, codec, std::placeholders::_1));
  cf_debug_string_funcs_.emplace(
      kInvertedIndexColumn,
      std::bind(&CodecImpl::DebugInvertedKey, codec, std::placeholders::_1));
  cf_debug_string_funcs_.emplace(
      kDocValueColumn,
      std::bind(&CodecImpl::DebugStoredFieldKey, codec, std::placeholders::_1));
  cf_debug_string_funcs_.emplace(
      kMetaColumn,
      std::bind(&CodecImpl::DebugMetaKey, codec, std::placeholders::_1));
  cf_debug_string_funcs_.emplace(kDictionaryColumn, do_nothing_func);
  cf_debug_string_funcs_.emplace(kPaxosLogColumn, do_nothing_func);
  cf_debug_string_funcs_.emplace(kPaxosDataMetaColumn, do_nothing_func);
  cf_debug_string_funcs_.emplace(kPaxosLogMetaColumn, do_nothing_func);
}

// mock api of Open()
bool VirtualDBMock::Open() { return true; }

// Get one snapshot
VirtualDBSnapshot* VirtualDBMock::NewSnapshot() {
  return new VirtualDBRocksSnapshot(nullptr);
}

// Release one snapshot
void VirtualDBMock::ReleaseSnapshot(VirtualDBSnapshot* snapshot) {
  delete snapshot;
}

// Parse protobuf string
SearchStatus VirtualDBMock::ParsePbString(
    const std::string& write_buffer_data) {
  std::lock_guard<std::mutex> guard(mut_);
  SearchStatus s;
  if (write_buffer_data.size() <= 0) {
    SearchLogError("Empty write_buffer_data");
    return s;
  }
  lsmsearch::MockDataList data_list;
  if (!data_list.ParseFromString(write_buffer_data)) {
    SearchLogError("RocksDBErrorStatus in parse write_buffer_data");
    s.SetStatus(kRocksDBErrorStatus,
                "RocksDBErrorStatus in parse write_buffer_data");
    return s;
  }
  for (size_t i = 0; i < data_list.mock_data_list_size(); ++i) {
    const lsmsearch::MockData& mock_data = data_list.mock_data_list(i);
    StorageColumnType cf =
        static_cast<StorageColumnType>(mock_data.column_type());
    KvList& kvs = cf_kv_list_[cf];
    const std::string& key = mock_data.key();
    const std::string& value = mock_data.value();
    const lsmsearch::MockData_Type& type = mock_data.type();
    SearchLogDebug("Put key[%d %s] value[%d %s] type[%d]", key.size(),
                   key.c_str(), value.size(), value.c_str(),
                   static_cast<int>(type));
    auto it = kvs.find(key);
    switch (type) {
      case lsmsearch::MockData::kUpdate: {
        if (it != kvs.end()) {
          it->second = value;
        } else {
          kvs.emplace(key, value);
        }
        break;
      }
      case lsmsearch::MockData::kMerge: {
        if (cf != kInvertedIndexColumn) {
          assert(false);
        }
        if (it != kvs.end()) {
          std::map<DocumentID, DocumentState, std::greater<uint64_t>> doclist;

          auto handle_doclist = [this](
                                    const std::string& doc_list_str,
                                    std::map<DocumentID, DocumentState,
                                             std::greater<uint64_t>>* doclist) {
            DocListReaderCodec* doc_list_reader = codec_->NewDocListReaderCodec(
                doc_list_str.c_str(), doc_list_str.size());
            FuncScopeGuard func_scope_guard([this, doc_list_reader]() {
              codec_->ReleaseDocListReaderCodec(doc_list_reader);
            });
            while (doc_list_reader->DocID() != DocIdSetIterator::NO_MORE_DOCS) {
              DocumentID doc_id = doc_list_reader->DocID();
              DocumentState doc_state = doc_list_reader->State();
              // kDocumentStateOK kDocumentStateDelete
              if (doc_state == kDocumentStateDelete) {
                doclist->erase(doc_id);
              } else {
                auto iter = doclist->find(doc_id);
                if (iter != doclist->end()) {
                  iter->second = doc_state;
                } else {
                  doclist->emplace(doc_id, doc_state);
                }
              }
              doc_list_reader->NextDoc();
            }
          };
          // 1. add old doclist
          handle_doclist(it->second, &doclist);
          // 2. add new doclist
          handle_doclist(value, &doclist);
          // 3. build value string
          // std::string empty_str;
          std::string final_value;
          DocListWriterCodec* final_doc_list =
              codec_->NewOrderDocListWriterCodec();
          FuncScopeGuard func_scope_guard([this, final_doc_list]() {
            codec_->ReleaseOrderDocListWriterCodec(final_doc_list);
          });
          for (const auto& doc_kv : doclist) {
            final_doc_list->AddDocID(doc_kv.first, doc_kv.second);
          }
          if (!final_doc_list->SerializeToBytes(final_value, 0)) {
            assert(false);
          }
          it->second = final_value;

          SearchLogDebug(
              "NotEmpty then Merge cf[%d] key(%s), value size(%d), "
              "value/doclist[%s]",
              mock_data.column_type(), codec_->DebugInvertedKey(key).c_str(),
              final_value.size(), final_doc_list->DebugString().c_str());

        } else {
          kvs.emplace(key, value);
          SearchLogDebug(
              "Empty then Put cf[%d] key(%s), value size(%d), "
              "value/doclist[%s]",
              mock_data.column_type(), codec_->DebugInvertedKey(key).c_str(),
              value.size(), DebugInvertedValueByReader(codec_, value).c_str());
        }
        break;
      }
      case lsmsearch::MockData::kDelete: {
        if (it != kvs.end()) {
          kvs.erase(it);
        }
        break;
      }
      case lsmsearch::MockData::kDeleteRange: {
        const std::string& end_key = mock_data.end_key();
        for (auto& kv_it : kvs) {
          const std::string& target_key = kv_it.first;
          if (target_key.compare(key) >= 0 && target_key.compare(end_key) < 0) {
            kvs.erase(target_key);
          }
        }
        break;
      }
      default: {
        SearchLogDebug("Unknown MockData type!");
        assert(false);
      }
    }
  }
  return s;
}

// Flush encoding bytes stream to write_buffer.
SearchStatus VirtualDBMock::FlushBuffer(WriteBuffer* write_buffer) {
  std::string append_write_buffer_data = write_buffer->Data();
  SearchLogDebug("FlushBuffer size = %d", append_write_buffer_data.size());
  return ParsePbString(append_write_buffer_data);
}

// Alloc one Write_buffer for write.
WriteBuffer* VirtualDBMock::NewWriteBuffer(const std::string* write_buffer) {
  WriteBuffer* buffer = nullptr;
  if (write_buffer != nullptr) {
    buffer = new WriteBufferMock(codec_, *write_buffer);
  } else {
    buffer = new WriteBufferMock(codec_);
  }
  SearchLogDebug("new NewWriteBuffer, %s", (write_buffer == nullptr)
                                               ? "empty write_buffer"
                                               : "not empty write_buffer");
  return buffer;
}

// Release one write_buffer
void VirtualDBMock::ReleaseWriteBuffer(WriteBuffer* buffer) { delete buffer; }

// Get one k/v
SearchStatus VirtualDBMock::Get(StorageColumnType column,
                                const std::string& key, std::string& value,
                                VirtualDBSnapshot* snapshot) {
  std::lock_guard<std::mutex> guard(mut_);
  SearchStatus s;
  KvList& kv_list = cf_kv_list_[column];
  auto iter = kv_list.find(key);
  if (iter != kv_list.end()) {
    value = iter->second;
  } else {
    s.SetStatus(kDocumentNotExistStatus, "");
  }
  SearchLogDebug("Get column[%d] key[%s] value[%s]", static_cast<int>(column),
                 key.c_str(), value.c_str());
  return s;
}

// Get multi k/v
void VirtualDBMock::MultiGet(std::vector<StorageColumnType> columns,
                             std::vector<std::string>& keys,
                             std::vector<std::string>& values,
                             std::vector<SearchStatus>& status,
                             VirtualDBSnapshot* snapshot) {
  SearchLogDebug("MultiGet size = %d", columns.size());
  for (int i = 0; i < columns.size(); ++i) {
    std::string value;
    status.push_back(Get(columns[i], keys[i], value, snapshot));
    values.push_back(value);
  }
}

// Get one iterator for read.
Iterator* VirtualDBMock::NewIterator(StorageColumnType column,
                                     VirtualDBReadOption* options) {
  class MockIterator : public Iterator {
   private:
    KvList kv_list_;
    KvList::iterator iter_;
    std::function<std::string(const std::string&)> func_;

   public:
    MockIterator(const KvList& kv_list,
                 const std::function<std::string(const std::string&)>& func)
        : kv_list_(kv_list), iter_(kv_list_.end()), func_(func) {
      // for (auto& kv : kv_list_) {
      //  SearchLogDebug("kv {[%d %s] => [%d %s]}", kv.first.size(),
      //                 func_(kv.first).c_str(), kv.second.size(),
      //                 kv.second.c_str());
      //}
      SearchLogDebug("kv list size = %d", kv_list.size());
    }

    ~MockIterator() {}

    bool Valid() const override { return iter_ != kv_list_.end(); }

    void SeekToFirst() override { iter_ = kv_list_.begin(); }

    void SeekToLast() override { iter_ = kv_list_.end(); }

    void Seek(const Slice& target) override {
      iter_ = kv_list_.lower_bound(target.ToString());
      if (iter_ == kv_list_.end()) {
        SearchLogDebug("Couldn't Seek target[%d]", target.size());
      } else {
        SearchLogDebug("target [%d ] Seek key[%d %s] value[%d %s]",
                       target.size(), iter_->first.size(),
                       func_(iter_->first).c_str(), iter_->second.size(),
                       iter_->second.c_str());
      }
    }

    void SeekForPrev(const Slice& target) override { assert(false); }

    void Next() override { ++iter_; }

    void Prev() override { std::prev(iter_, 1); }

    Slice key() const override { return iter_->first; }

    Slice value() const override { return iter_->second; }

    SearchStatus status() const override { return SearchStatus(); }
  };
  std::lock_guard<std::mutex> guard(mut_);
  return new MockIterator(cf_kv_list_[column], cf_debug_string_funcs_[column]);
}

// mock api
SearchStatus VirtualDBMock::CompactRange(StorageColumnType column,
                                         const std::string& begin,
                                         const std::string& end) {
  SearchStatus status;
  return status;
}

// mock api
SearchStatus VirtualDBMock::DropDB() { return SearchStatus(); }
}  // namespace wwsearch
