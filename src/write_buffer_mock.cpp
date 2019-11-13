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

#include "write_buffer_mock.h"
#include "func_scope_guard.h"
#include "logger.h"
#include "utils.h"

namespace wwsearch {

WriteBufferMock::WriteBufferMock(CodecImpl* codec, const std::string& data)
    : codec_(codec), data_(data) {
  DeSeriableData();
}

// Write one k/v in memory maps.
SearchStatus WriteBufferMock::Put(StorageColumnType column,
                                  const std::string& key,
                                  const std::string& value) {
  SearchStatus s;
  KvList& kvs = cf_kv_list_[column];
  auto iter = kvs.find(key);
  if (iter != kvs.end()) {
    iter->second.first = value;
    iter->second.second = lsmsearch::MockData::kUpdate;
  } else {
    kvs.emplace(key, std::make_pair(value, lsmsearch::MockData::kUpdate));
    kv_cnt_++;
  }
  return s;
}

// Delete one k/v in memory maps.
SearchStatus WriteBufferMock::Delete(StorageColumnType column,
                                     const std::string& key) {
  SearchStatus s;
  KvList& kvs = cf_kv_list_[column];
  kvs.erase(key);
  kvs.emplace(key, std::make_pair("", lsmsearch::MockData::kDelete));
  kv_cnt_--;
  return s;
}

// Delete range keys in memory maps.
SearchStatus WriteBufferMock::DeleteRange(StorageColumnType column,
                                          const std::string& begin_key,
                                          const std::string& end_key) {
  SearchStatus s;
  DeleteKeyRangeList& delete_keyrange_list = cf_delete_keyrange_list_[column];
  delete_keyrange_list.emplace(std::make_pair(begin_key, end_key));
  kv_cnt_--;
  return s;
}

// Serialize
SearchStatus WriteBufferMock::SeriableData() {
  SearchStatus s;
  lsmsearch::MockDataList data_list;
  data_.clear();
  kv_cnt_ = 0;
  total_size_ = 0;
  for (auto& type_it : cf_kv_list_) {
    for (auto& kv_it : type_it.second) {
      lsmsearch::MockData* mock_data = data_list.add_mock_data_list();
      mock_data->set_column_type(type_it.first);
      mock_data->set_key(kv_it.first);
      mock_data->set_value(kv_it.second.first);
      mock_data->set_type(kv_it.second.second);
      SearchLogDebug("mock data cf[%d], key[%s], value[%s], type[%d]",
                     type_it.first, kv_it.first.c_str(),
                     kv_it.second.first.c_str(),
                     static_cast<int>(kv_it.second.second));
      kv_cnt_++;
    }
  }

  for (auto& type_it : cf_delete_keyrange_list_) {
    for (auto& keyrange_it : type_it.second) {
      lsmsearch::MockData* mock_data = data_list.add_mock_data_list();
      mock_data->set_column_type(type_it.first);
      mock_data->set_key(keyrange_it.first);
      mock_data->set_end_key(keyrange_it.second);
      mock_data->set_value("");
      mock_data->set_type(lsmsearch::MockData::kDeleteRange);
    }
  }
  bool ret = data_list.SerializeToString(&data_);
  if (!ret) {
    SearchLogError("WriteBufferMock MockDataList SerializeToString failed!!!");
    s.SetStatus(kRocksDBErrorStatus,
                "RocksDBErrorStatus WriteBufferMock::Append");
  } else {
    total_size_ = data_.size();
  }
  return s;
}

SearchStatus WriteBufferMock::DeSeriableData() {
  SearchStatus s;
  if (data_.size() <= 0) {
    SearchLogError("DeSeriableData encounter empty data");
    return s;
  }
  kv_cnt_ = 0;
  total_size_ = data_.size();
  lsmsearch::MockDataList data_list;
  if (!data_list.ParseFromString(data_)) {
    s.SetStatus(kRocksDBErrorStatus,
                "RocksDBErrorStatus WriteBufferMock::Append");
  }
  if (s.OK()) {
    for (size_t i = 0; i < data_list.mock_data_list_size(); ++i) {
      const lsmsearch::MockData& mock_data = data_list.mock_data_list(i);
      s = Put(static_cast<StorageColumnType>(mock_data.column_type()),
              mock_data.key(), mock_data.value());
      if (!s.OK()) {
        break;
      }
    }
  }
  return s;
}

// merge one key's value into previous in memory map.
SearchStatus WriteBufferMock::Merge(StorageColumnType column,
                                    const std::string& key,
                                    const std::string& value) {
  SearchLogDebug("");
  SearchStatus s;
  KvList& kvs = cf_kv_list_[column];
  auto iter = kvs.find(key);
  if (column != kInvertedIndexColumn) {
    assert(false);
  }
  if (iter != kvs.end()) {
    std::pair<std::string, lsmsearch::MockData_Type>& value_type_pair =
        iter->second;

    std::map<DocumentID, DocumentState, std::greater<uint64_t>> doclist;

    auto handle_doclist = [this](const std::string& doc_list_str,
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

        auto iter = doclist->find(doc_id);
        if (iter != doclist->end()) {
          iter->second = doc_state;
        } else {
          doclist->emplace(doc_id, doc_state);
        }

        doc_list_reader->NextDoc();
      }
    };
    // 1. add old doclist
    handle_doclist(value_type_pair.first, &doclist);
    // 2. add new doclist
    handle_doclist(value, &doclist);
    // 3. build value string
    // std::string empty_str;
    std::string final_value;
    DocListWriterCodec* final_doc_list = codec_->NewOrderDocListWriterCodec();
    FuncScopeGuard func_scope_guard([this, final_doc_list]() {
      codec_->ReleaseOrderDocListWriterCodec(final_doc_list);
    });
    for (const auto& doc_kv : doclist) {
      final_doc_list->AddDocID(doc_kv.first, doc_kv.second);
    }
    if (!final_doc_list->SerializeToBytes(final_value, 0)) {
      assert(false);
    }

    SearchLogDebug("Merge cf[%d] key(%s), value size(%d), value/doclist[%s]",
                   column, codec_->DebugInvertedKey(key).c_str(),
                   final_value.size(), final_doc_list->DebugString().c_str());

    value_type_pair.first = final_value;
    value_type_pair.second = lsmsearch::MockData::kMerge;
  } else {
    kvs.emplace(key, std::make_pair(value, lsmsearch::MockData::kMerge));
    SearchLogDebug(
        "Merge Put cf[%d] key(%s), value size(%d), "
        "value/doclist[%s]",
        column, codec_->DebugInvertedKey(key).c_str(), value.size(),
        DebugInvertedValueByReader(codec_, value).c_str());
    kv_cnt_++;
  }
  SearchLogDebug("");
  return s;
}

SearchStatus WriteBufferMock::Append(WriteBuffer* prepare_append_write_buffer) {
  SearchStatus s;
  std::string append_write_buffer_data = prepare_append_write_buffer->Data();
  if (append_write_buffer_data.size() <= 0) {
    // s.SetStatus(kRocksDBErrorStatus, "Set empty append_write_buffer_data");
    return s;
  }
  lsmsearch::MockDataList data_list;
  if (!data_list.ParseFromString(append_write_buffer_data)) {
    s.SetStatus(kRocksDBErrorStatus,
                "RocksDBErrorStatus WriteBufferMock::Append");
    return s;
  }
  for (size_t i = 0; i < data_list.mock_data_list_size(); ++i) {
    const lsmsearch::MockData& mock_data = data_list.mock_data_list(i);
    StorageColumnType cf =
        static_cast<StorageColumnType>(mock_data.column_type());
    const lsmsearch::MockData_Type& type = mock_data.type();
    switch (type) {
      case lsmsearch::MockData::kUpdate: {
        s = Put(cf, mock_data.key(), mock_data.value());
        break;
      }
      case lsmsearch::MockData::kMerge: {
        if (cf != kInvertedIndexColumn) {
          assert(false);
        }
        s = Merge(cf, mock_data.key(), mock_data.value());
        break;
      }
      case lsmsearch::MockData::kDelete: {
        s = Delete(cf, mock_data.key());
        break;
      }
      default:
        assert(false);
    }

    if (!s.OK()) {
      break;
    }
  }
  return s;
}

std::string WriteBufferMock::Data() {
  SearchStatus s;
  s = SeriableData();
  if (!s.OK()) {
    SearchLogError("BuildSeriableData error = %d, state = %s", s.GetCode(),
                   s.GetState().c_str());
  }
  return s.OK() ? data_ : "";
}

uint64_t WriteBufferMock::DataSize() const { return total_size_; }

uint64_t WriteBufferMock::KvCount() const { return kv_cnt_; }

}  // namespace wwsearch
