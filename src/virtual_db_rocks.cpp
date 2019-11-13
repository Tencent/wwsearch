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

#include "virtual_db_rocks.h"
#include "codec_doclist_impl.h"
#include "index_config.h"
#include "iterator_rocks.h"
#include "logger.h"
#include "write_buffer_rocks.h"

#include <memory>
#include "codec_doclist_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/db_ttl.h"

namespace wwsearch {

namespace merge {
using namespace wwsearch;

typedef struct HeapItem {
  DocList* ptr_;
  size_t left_size_;
  uint32_t priority_;

  inline void Set(DocList* ptr, size_t size, uint32_t priority) {
    //  version update
    char* pc = (char*)ptr;
    pc += sizeof(DocListHeader);
    ptr_ = (DocList*)pc;
    left_size_ = size - sizeof(DocListHeader);
    priority_ = priority;
  }

  inline void Swap(HeapItem& o) {
    DocList* t2 = o.ptr_;
    o.ptr_ = ptr_;
    ptr_ = t2;

    left_size_ ^= o.left_size_;
    o.left_size_ ^= left_size_;
    left_size_ ^= o.left_size_;

    priority_ ^= o.priority_;
    o.priority_ ^= priority_;
    priority_ ^= o.priority_;
  }
} HeapItem;

#define MergeOperator_LOCKS_SIZE (1000)
#define MergeOperator_HEAP_SIZE (1000)

#define DOCID(a) ((a).left_size_ == 0 ? 0 : (a).ptr_->doc_id_)

#define HeapItemGreater(a, b) \
  ((DOCID(a) > DOCID(b)) || (DOCID(a) == DOCID(b) && a.priority_ > b.priority_))

class OptimizeMerger {
 public:
  std::atomic<std::uint64_t> seq_;
  std::mutex locks_[MergeOperator_LOCKS_SIZE];
  merge::HeapItem* heaps_[MergeOperator_LOCKS_SIZE];
  uint32_t max_doc_list_num_;

 public:
  OptimizeMerger(uint32_t max_doc_list_num)
      : max_doc_list_num_(max_doc_list_num) {
    assert(max_doc_list_num_ > 0);
    for (size_t i = 0; i < MergeOperator_LOCKS_SIZE; i++) {
      std::lock_guard<std::mutex> guard(locks_[i]);
      heaps_[i] = new merge::HeapItem[MergeOperator_HEAP_SIZE];
    }
  }

  virtual ~OptimizeMerger() {
    for (size_t i = 0; i < MergeOperator_LOCKS_SIZE; i++) {
      std::lock_guard<std::mutex> guard(locks_[i]);
      if (heaps_[i] != nullptr) delete heaps_[i];
      heaps_[i] = nullptr;
    }
  }

  inline void HeapShiftUp(HeapItem* items, size_t size, size_t pos) {
    while (pos != 0) {
      size_t parent = (pos - 1) / 2;
      if (HeapItemGreater(items[pos], items[parent])) {
        items[pos].Swap(items[parent]);
        pos = parent;
      } else {
        break;
      }
    }
  }

  inline void HeapShiftDown(HeapItem* items, size_t size, size_t pos) {
    for (;;) {
      size_t left = pos * 2 + 1;
      size_t right = pos * 2 + 2;
      if (right < size) {
        if (HeapItemGreater(items[left], items[right])) {
          // 1. left big
          if (HeapItemGreater(items[left], items[pos])) {
            // 1.1 left biggest
            items[left].Swap(items[pos]);
            pos = left;
          } else {
            break;
          }
        } else if (HeapItemGreater(items[right], items[pos])) {
          // 2. right biggest
          items[right].Swap(items[pos]);
          pos = right;
        } else {
          // 3. pos biggest
          break;
        }
      } else if (left < size) {
        // 1. left biggest
        if (HeapItemGreater(items[left], items[pos])) {
          items[left].Swap(items[pos]);
          pos = left;
        } else {
          // 2. pos biggest
          break;
        }
      } else {
        // 1. pos biggest,reach tail
        break;
      }
    }
  }

  inline bool OptimizeDocListMerge(Codec* codec, std::string* new_value,
                                   HeapItem* items, size_t queue_size,
                                   size_t approximate_size,
                                   bool purge_deletedoc) {
#define ONE_DOCID_SIZE (9)
    assert(sizeof(DocList) == ONE_DOCID_SIZE);
    DocumentID doc_id;

    // build heap
    for (uint32_t j = 0; j < queue_size; j++) {
      HeapShiftUp(items, queue_size, j);
    }

    uint64_t match_doc_count = 0;
    uint64_t match_delete_doc_count = 0;
    DocListWriterCodec* writer = codec->NewOrderDocListWriterCodec();
    // Attention:
    // Even if the total size of doc list reach max doc list count,we can not
    // delete tail's delete id in PartialMerge. But we can do that in FullMerge.
    while (DOCID(items[0]) != 0) {
      DocList* doc = items[0].ptr_;
      if (purge_deletedoc) {
        // full merge
        // if reach max ,just break
        if (items[0].ptr_->doc_state_ != kDocumentStateDelete) {
          writer->AddDocID(doc->doc_id_, doc->doc_state_);
          // new_value->append((const char*)items[0].ptr_, ONE_DOCID_SIZE);
          match_doc_count++;
          if (match_doc_count >= this->max_doc_list_num_) {
            SearchLogError("MatchDocCount reach max limit=%lu",
                           this->max_doc_list_num_);
            break;
          }
        }
      } else {
        // partial merge
        // keep max num undelete doc id and all delete doc id
        if (items[0].ptr_->doc_state_ != kDocumentStateDelete) {
          if (match_doc_count < this->max_doc_list_num_) {
            writer->AddDocID(doc->doc_id_, doc->doc_state_);
            // new_value->append((const char*)items[0].ptr_, ONE_DOCID_SIZE);
            match_doc_count++;
          }
        } else {
          writer->AddDocID(doc->doc_id_, doc->doc_state_);
          // new_value->append((const char*)items[0].ptr_, ONE_DOCID_SIZE);
          match_delete_doc_count++;

          // protect our system?
          if (match_delete_doc_count > 10000000) {
            SearchLogError("too many delete doc id keep");
            break;
          }
        }
      }

      doc_id = DOCID(items[0]);
      // skip same doc_id
      do {
        items[0].ptr_++;
        items[0].left_size_ -= ONE_DOCID_SIZE;
        HeapShiftDown(items, queue_size, 0);
      } while (DOCID(items[0]) == doc_id);
    }

    bool ret = writer->SerializeToBytes(*new_value, 0);
    codec->ReleaseOrderDocListWriterCodec(writer);
    SearchLogDebug("new_value len:%u", new_value->size()) return ret;
  }
};

}  // namespace merge

static bool FastMerge(const rocksdb::Slice* existing_value,
                      const std::deque<std::string>& operand_list,
                      std::string* new_value) {
  if (existing_value != NULL && existing_value != nullptr)
    new_value->assign(existing_value->data(), existing_value->size());
  for (size_t i = 0; i < operand_list.size(); i++) {
    new_value->append(operand_list[i]);
  }
  if (new_value->size() > 10000000) {
    new_value->resize(10000000);
  }
  return true;
}

DocListMergeOperator::~DocListMergeOperator() {
  if (nullptr != merger_) delete merger_;
  merger_ = nullptr;
}

// merger's life cycle manage by DocListMergeOperator
// DocListMergeOperator life cycle manage by outside shared_ptr
DocListMergeOperator* DocListMergeOperator::NewInstance(VDBParams* params) {
  merge::OptimizeMerger* merger =
      new merge::OptimizeMerger(params->max_doc_list_num_);
  DocListMergeOperator* instance =
      new DocListMergeOperator(params->codec_, merger);
  return instance;
}

bool DocListMergeOperator::FullMergeV2(
    const rocksdb::MergeOperator::MergeOperationInput& merge_in,
    rocksdb::MergeOperator::MergeOperationOutput* merge_out) const {
  SearchLogDebug("new value size:%u", merge_out->new_value.size());

  // collect buffer
  std::vector<std::pair<merge::DocList*, size_t>> doc_lists;
  std::vector<std::string> alloc_buffer;
  if (merge_in.existing_value) {
    doc_lists.push_back(DecodeDocList(alloc_buffer,
                                      merge_in.existing_value->data(),
                                      merge_in.existing_value->size()));
  }
  for (size_t i = 0, length = merge_in.operand_list.size(); i < length; i++) {
    doc_lists.push_back(DecodeDocList(alloc_buffer,
                                      merge_in.operand_list[i].data(),
                                      merge_in.operand_list[i].size()));
    SearchLogDebug("in doclist size:%u", merge_in.operand_list[i].size())
  }

  // must clear the value.
  merge_out->new_value.clear();

  // init head and merge
  bool ret = DoMerge(&(merge_out->new_value), doc_lists, true);
  SearchLogDebug("ret:%d size:%u", ret, alloc_buffer.size());
  return ret;
}

bool DocListMergeOperator::PartialMergeMulti(
    const rocksdb::Slice& key, const std::deque<rocksdb::Slice>& operand_list,
    std::string* new_value, rocksdb::Logger* logger) const {
  SearchLogDebug("new value size:%u", new_value->size());
  // assert(new_value->size() == 0);
  new_value->clear();

  // collect buffer
  std::vector<std::pair<merge::DocList*, size_t>> doc_lists;
  std::vector<std::string> alloc_buffer;
  for (size_t i = 0, length = operand_list.size(); i < length; i++) {
    doc_lists.push_back(DecodeDocList(alloc_buffer, operand_list[i].data(),
                                      operand_list[i].size()));
    SearchLogDebug("in doclist size:%u", operand_list[i].size())
  }

  // init head and merge
  bool ret = DoMerge(new_value, doc_lists, false);
  SearchLogDebug("ret:%d size:%u", ret, alloc_buffer.size());
  return ret;
}

// Decode [data] to FixDocList,If alloc string,string will store to
// alloc_buffer.
std::pair<merge::DocList*, size_t> DocListMergeOperator::DecodeDocList(
    std::vector<std::string>& alloc_buffer, const char* data,
    size_t data_len) const {
  assert(data_len < (static_cast<uint32_t>(1) << 31));

  bool ret;
  bool use_buffer;
  std::string buffer;
  ret = codec_->DecodeDocListToFixBytes(data, data_len, use_buffer, buffer);
  if (!ret) {
    SearchLogError("DecodeDocList fail,Data Broken or Bug ?");
    assert(false);
  }
  std::pair<merge::DocList*, size_t> one_list;
  if (use_buffer) {
    alloc_buffer.push_back(std::move(buffer));
    one_list.first = (merge::DocList*)alloc_buffer.back().c_str();
    one_list.second = alloc_buffer.back().size();
  } else {
    one_list.first = (merge::DocList*)data;
    one_list.second = data_len;
  }
  return one_list;
}

bool DocListMergeOperator::DoMerge(
    std::string* new_value,
    std::vector<std::pair<merge::DocList*, size_t>>& doc_lists,
    bool full_merge) const {
  size_t list_size = doc_lists.size();
  size_t approximate_size = 0;
  bool ret;
  if (list_size < MergeOperator_HEAP_SIZE) {
    std::uint64_t pos = merger_->seq_.fetch_add(1, std::memory_order_relaxed) %
                        MergeOperator_LOCKS_SIZE;
    std::lock_guard<std::mutex> lock_guard(merger_->locks_[pos]);
    merge::HeapItem* heap = merger_->heaps_[pos];

    for (size_t i = 0; i < list_size; i++) {
      heap[i].Set((merge::DocList*)doc_lists[i].first, doc_lists[i].second, i);
      SearchLogDebug("in doclist size:%u", doc_lists[i].second);
      approximate_size += doc_lists[i].second;
    }

    // 1/3 compression ?
    approximate_size /= 3;
    ret = merger_->OptimizeDocListMerge(codec_, new_value, heap, list_size,
                                        approximate_size, full_merge);
  } else {
    // WTF,why so big
    SearchLogError("too many list to merge:%u", list_size);
    merge::HeapItem* heap = new merge::HeapItem[list_size];
    for (size_t i = 0; i < list_size; i++) {
      heap[i].Set((merge::DocList*)doc_lists[i].first, doc_lists[i].second, i);
      SearchLogDebug("in doclist size:%u", doc_lists[i].second);
      approximate_size += doc_lists[i].second;
    }

    // 1/3 compression ?
    approximate_size /= 3;
    ret = merger_->OptimizeDocListMerge(codec_, new_value, heap, list_size,
                                        approximate_size, full_merge);
    delete heap;
  }
  return ret;
}

bool DocListMergeOperator::FullMerge(
    const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
    const std::deque<std::string>& operand_list, std::string* new_value,
    rocksdb::Logger* logger) const {
  // deprecated
  assert(false);
#ifdef BENCHMARK
  return FastMerge(existing_value, operand_list, new_value);
#else
  SearchLogDebug("");

  std::vector<DocListReaderCodec*> priority_queue;
  for (size_t i = 0; i < operand_list.size(); i++) {
    const std::string& value = operand_list[i];
    DocListReaderCodec* item =
        codec_->NewDocListReaderCodec(value.c_str(), value.size());
    item->SetPriority(i);
    priority_queue.push_back(item);
  }

  bool ok = DocListMerge(priority_queue, new_value, logger, true);
  if (!ok) {
    SearchLogError("DocListMerge fail");
    assert(false);
  }

  for (auto item : priority_queue) {
    codec_->ReleaseDocListReaderCodec(item);
  }
  return true;
#endif
}

bool DocListMergeOperator::PartialMerge(const rocksdb::Slice& key,
                                        const rocksdb::Slice& left_operand,
                                        const rocksdb::Slice& right_operand,
                                        std::string* new_value,
                                        rocksdb::Logger* logger) const {
  // deprecated
  assert(false);
#ifdef BENCHMARK
  new_value->append(left_operand.data(), left_operand.size());
  new_value->append(right_operand.data(), right_operand.size());
  if (new_value->size() > 10000000) {
    new_value->resize(10000000);
  }
  return true;
#else

  SearchLogDebug("");
  // Optimize, only do partial merge while operand is big.
  std::vector<DocListReaderCodec*> priority_queue;
  {
    DocListReaderCodec* item =
        codec_->NewDocListReaderCodec(left_operand.data(), left_operand.size());
    item->SetPriority(0);
    priority_queue.push_back(item);
  }
  {
    DocListReaderCodec* item = codec_->NewDocListReaderCodec(
        right_operand.data(), right_operand.size());
    item->SetPriority(1);
    priority_queue.push_back(item);
  }

  bool ok = DocListMerge(priority_queue, new_value, logger, false);
  if (!ok) {
    SearchLogError("DocListMerge fail");
    assert(false);
  }

  for (auto item : priority_queue) {
    codec_->ReleaseDocListReaderCodec(item);
  }
  return true;
#endif
}

bool DocListMergeOperator::DocListMerge(std::vector<DocListReaderCodec*>& items,
                                        std::string* new_value,
                                        rocksdb::Logger* logger,
                                        bool purge_deleted) const {
  SearchLogDebug("");
  assert(false);
  DocListWriterCodec* writer = codec_->NewOrderDocListWriterCodec();
  // DocListOrderWriterCodecImpl writer(new_value);
  std::make_heap(items.begin(), items.end(), codec_->GetGreaterComparator());

  for (;;) {
    std::pop_heap(items.begin(), items.end(), codec_->GetGreaterComparator());
    DocListReaderCodec* back = items.back();
    DocumentID last_document_id = back->DocID();
    if (last_document_id == DocIdSetIterator::NO_MORE_DOCS) break;
    if (back->State() == kDocumentStateDelete && purge_deleted) {
      // delete doc
      SearchLogDebug("delete doc[%lu]", last_document_id);
    } else {
      writer->AddDocID(back->DocID(), back->State());
      SearchLogDebug("merge doc[%lu]", last_document_id);
    }

    items.back()->NextDoc();
    // skip same doc
    for (;;) {
      std::push_heap(items.begin(), items.end(),
                     codec_->GetGreaterComparator());
      if (items.back()->DocID() == last_document_id) {
        std::pop_heap(items.begin(), items.end(),
                      codec_->GetGreaterComparator());
        items.back()->NextDoc();
        SearchLogDebug("skip same doc[%lu]", last_document_id);
      } else {
        break;
      }
    }
  }
  assert(writer->SerializeToBytes(*new_value, 0));
  codec_->ReleaseOrderDocListWriterCodec(writer);
  return true;
}

// rocksdb must have default column. So:
// default -> kStoredFieldColumn
static std::string column_family_mapping[kMaxColumn] = {"default",
                                                        "kInvertedIndexColumn",
                                                        "kDocValueColumn",
                                                        "kMetaColumn",
                                                        "kDictionaryColumn",
                                                        "kPaxosLogColumn",
                                                        "kPaxosDataMetaColumn",
                                                        "kPaxosLogMetaColumn"};

VirtualDBRocksImpl::VirtualDBRocksImpl(
    VDBParams* params, const std::shared_ptr<DbRocksWriteQueue>& write_queue)
    : params_(params),
      db_(nullptr),
      merger_(nullptr),
      write_queue_(write_queue) {}

VirtualDBRocksImpl::~VirtualDBRocksImpl() { Clear(); }

bool VirtualDBRocksImpl::Open() {
  if (nullptr != this->db_) {
    SearchLogError("DB had opened");
    this->SetState("DB had opened!");
    return false;
  }

  InitDBOptions();

  std::vector<int32_t> ttls;
  column_families_.clear();

  // Couldn't use HashSkipListRepFactory for some unknown bug
  if (options_.allow_concurrent_memtable_write == false) {
    options_.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(9));
    options_.memtable_factory.reset(rocksdb::NewHashSkipListRepFactory());
  }

  // options_.disable_auto_compactions = true;

  for (int i = 0; i < kMaxColumn; i++) {
    rocksdb::ColumnFamilyOptions cf_options =
        NewColumnFamilyOptions(options_, (StorageColumnType)i);
    column_families_.push_back(
        rocksdb::ColumnFamilyDescriptor(column_family_mapping[i], cf_options));
    /*
    if (i == kPaxosLogColumn) {
      ttls.push_back(this->params_->plog_ttl_seconds_);  // in seconds.
    } else {
      ttls.push_back(-1);  // infinite seconds.
    }
    */
  }

  rocksdb::Status s =
      rocksdb::DB::Open(this->options_, this->params_->path, column_families_,
                        &(this->column_famil_handles_), &this->db_);
  // ttl open
  // auto s = rocksdb::DBWithTTL::Open(
  //  this->options_, this->params_->path, column_families,
  // &(this->column_famil_handles_), &this->db_, ttls, false);

  if (!s.ok()) {
    SearchLogError("DB open failed %s", s.getState());
    this->SetState(s.getState());
    return false;
  }

  return true;
}

std::vector<rocksdb::ColumnFamilyHandle*>&
VirtualDBRocksImpl::ColumnFamilyHandle() {
  return this->column_famil_handles_;
}

VirtualDBSnapshot* VirtualDBRocksImpl::NewSnapshot() {
  const rocksdb::Snapshot* snapshot = this->db_->GetSnapshot();
  assert(snapshot != nullptr);
  return new VirtualDBRocksSnapshot(snapshot);
}

void VirtualDBRocksImpl::ReleaseSnapshot(VirtualDBSnapshot* snapshot) {
  VirtualDBRocksSnapshot* real_snapshot =
      reinterpret_cast<VirtualDBRocksSnapshot*>(snapshot);
  this->db_->ReleaseSnapshot(real_snapshot->GetSnapshot());
  delete snapshot;
}

SearchStatus VirtualDBRocksImpl::FlushBuffer(WriteBuffer* write_buffer) {
  SearchStatus status;
  if (!write_queue_) {
    WriteBufferRocksImpl* buffer =
        reinterpret_cast<WriteBufferRocksImpl*>(write_buffer);
    auto s = this->db_->Write(rocksdb::WriteOptions(), buffer->write_batch_);
    if (!s.ok()) {
      status.SetStatus(kRocksDBErrorStatus, s.getState());
    }
  } else {
    VirtualDBRocksWriteOption write_options;
    status = write_queue_->Write(this, &write_options, write_buffer);
  }
  return status;
}

WriteBuffer* VirtualDBRocksImpl::NewWriteBuffer(
    const std::string* write_buffer) {
  return new WriteBufferRocksImpl(&column_famil_handles_, NULL, write_buffer);
}

void VirtualDBRocksImpl::ReleaseWriteBuffer(WriteBuffer* buffer) {
  delete buffer;
}

SearchStatus VirtualDBRocksImpl::Get(StorageColumnType column,
                                     const std::string& key, std::string& value,
                                     VirtualDBSnapshot* snapshot) {
  rocksdb::Status ss;
  rocksdb::ReadOptions read_option;

  if (nullptr != snapshot) {
    VirtualDBRocksSnapshot* real_snapshot =
        reinterpret_cast<VirtualDBRocksSnapshot*>(snapshot);
    read_option.snapshot = real_snapshot->GetSnapshot();
  }
  ss = this->db_->Get(read_option, this->column_famil_handles_[column], key,
                      &value);
  SearchStatus status;
  if (ss.ok()) {
    status.SetStatus(kOK, "");
  } else if (ss.IsNotFound()) {
    status.SetStatus(kDocumentNotExistStatus, "");
  } else {
    status.SetStatus(kRocksDBErrorStatus, ss.getState());
  }
  return status;
}

void VirtualDBRocksImpl::MultiGet(std::vector<StorageColumnType> columns,
                                  std::vector<std::string>& keys,
                                  std::vector<std::string>& values,
                                  std::vector<SearchStatus>& status,
                                  VirtualDBSnapshot* snapshot) {
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::vector<rocksdb::Slice> slice_keys;
  assert(keys.size() == columns.size());
  for (size_t i = 0; i < columns.size(); i++) {
    handles.push_back(this->column_famil_handles_[columns[i]]);
    slice_keys.push_back(keys[i]);
  }
  std::vector<rocksdb::Status> ss;
  rocksdb::ReadOptions read_option;

  if (nullptr != snapshot) {
    VirtualDBRocksSnapshot* real_snapshot =
        reinterpret_cast<VirtualDBRocksSnapshot*>(snapshot);
    read_option.snapshot = real_snapshot->GetSnapshot();
  }
  ss = this->db_->MultiGet(read_option, handles, slice_keys, &values);

  assert(ss.size() == keys.size());
  for (rocksdb::Status s : ss) {
    SearchLogDebug("rocksdb status[%s]", s.getState());

    SearchStatus temp;
    if (s.ok()) {
      temp.SetStatus(kOK, "");
    } else if (s.IsNotFound()) {
      temp.SetStatus(kDocumentNotExistStatus, "");
    } else {
      temp.SetStatus(kRocksDBErrorStatus, s.getState());
    }
    status.push_back(temp);
  }
}

void VirtualDBRocksImpl::InitDBOptions() {
  // options_.IncreaseParallelism();
  options_.OptimizeLevelStyleCompaction();
  options_.create_if_missing = true;
  options_.create_missing_column_families = true;
  // file size
  options_.max_subcompactions = params_->rocks_max_subcompactions;

  // If use HashSkipListMemtable wouldn't support concurrent_memtable_write
  options_.allow_concurrent_memtable_write =
      params_->rocks_allow_concurrent_memtable_write;

  // log settings
  options_.max_log_file_size = params_->rocks_max_log_file_size;  // 50MB
  options_.log_file_time_to_roll =
      params_->rocks_log_file_time_to_roll;                       // one hours
  options_.keep_log_file_num = params_->rocks_keep_log_file_num;  // 10 days

  // manifest
  options_.max_manifest_file_size =
      params_->rocks_max_manifest_file_size;  // 50MB

  // write
  options_.level0_file_num_compaction_trigger =
      params_->rocks_level0_file_num_compaction_trigger;
  options_.level0_slowdown_writes_trigger =
      params_->rocks_level0_slowdown_writes_trigger;
  options_.level0_stop_writes_trigger =
      params_->rocks_level0_stop_writes_trigger;
  options_.num_levels = params_->rocks_num_levels;
  options_.write_buffer_size = params_->rocks_write_buffer_size;
  options_.max_write_buffer_number =
      params_->rocks_max_write_buffer_number;  // 3 write buffer
  options_.max_total_wal_size = params_->rocks_max_total_wal_size;  // 200 MB
  options_.db_write_buffer_size =
      params_->rocks_db_write_buffer_size;  // 200 MB
  options_.max_bytes_for_level_base = params_->rocks_max_bytes_for_level_base;
  // options_.max_bytes_for_level_multiplier = 10; // *10
  // options_.wal_bytes_per_sync = params_->rocks_20 << 20; // 2MB sync once ?
  // options_.bytes_per_sync = 20 << 20;

  // Doesn't need anymore if use write_queue for colib
  // options_.enable_write_thread_adaptive_yield = false;
  // options_.write_thread_max_yield_usec = 0;
  // options_.write_thread_slow_yield_usec = 0;

  options_.max_background_jobs = params_->rocks_max_background_jobs_;

  // table cache block cache
  {
    rocksdb::BlockBasedTableOptions table_options;
    if (params_->rocks_db_block_cache_bytes != 0) {
      table_options.block_cache =
          rocksdb::NewLRUCache(params_->rocks_db_block_cache_bytes);
    }
    table_options.cache_index_and_filter_blocks =
        params_->rocks_db_cache_index_and_filter_blocks;
    if (params_->rocks_db_max_open_files != 0) {
      options_.max_open_files = params_->rocks_db_max_open_files;
    }
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  // compression
  options_.compression = params_->rocks_compression;

  // Open all staticstic info for DB
  options_.statistics = rocksdb::CreateDBStatistics();
}

rocksdb::ColumnFamilyOptions VirtualDBRocksImpl::NewColumnFamilyOptions(
    const rocksdb::Options& options, StorageColumnType column) {
  rocksdb::ColumnFamilyOptions cf_options(options);
  switch (column) {
    case kInvertedIndexColumn:
      assert(merger_ == nullptr);
      merger_ = DocListMergeOperator::NewInstance(this->params_);
      cf_options.merge_operator.reset(merger_);
      break;
    default:
      break;
  }
  auto it = this->params_->columns_compactionfilter.find(column);
  if (it != this->params_->columns_compactionfilter.end()) {
    cf_options.compaction_filter = it->second;
  }

  return cf_options;
}

Iterator* VirtualDBRocksImpl::NewIterator(StorageColumnType column,
                                          VirtualDBReadOption* options) {
  VirtualDBRocksSnapshot* snapshot =
      (VirtualDBRocksSnapshot*)options->snapshot_;
  rocksdb::ReadOptions rocks_options;
  if (nullptr != snapshot) {
    rocks_options.snapshot = snapshot->GetSnapshot();
  }

  if (column >= this->column_famil_handles_.size()) return nullptr;

  auto rocks_iterator = this->db_->NewIterator(
      rocks_options, this->column_famil_handles_[column]);
  if (nullptr == rocks_iterator) return nullptr;
  return new IteratorRocks(rocks_iterator);
}

SearchStatus VirtualDBRocksImpl::CompactRange(StorageColumnType column,
                                              const std::string& begin,
                                              const std::string& end) {
  SearchStatus status;
  rocksdb::ColumnFamilyHandle* cf_handle = this->column_famil_handles_[column];
  rocksdb::CompactRangeOptions options;
  rocksdb::Slice begin_slice(begin);
  rocksdb::Slice end_slice(end);
  rocksdb::Status rocksdb_s =
      this->db_->CompactRange(options, cf_handle, &begin_slice, &end_slice);
  if (!rocksdb_s.ok()) {
    status.SetStatus(kRocksDBErrorStatus, rocksdb_s.getState());
  }
  return status;
}

SearchStatus VirtualDBRocksImpl::DropDB() {
  Clear();
  SearchStatus status;
  // auto ret = this->db_->DropColumnFamilies(this->column_famil_handles_);
  // status.SetStatus(ret.code(), ret.getState());
  auto ss = rocksdb::DestroyDB(this->params_->path, this->options_);
  status.SetStatus(ss.code(), "");
  return status;
}

}  // namespace wwsearch
