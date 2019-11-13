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

#include <time.h>
#include <thread>

#include "bench_index.h"
#include "random_creater.h"
#include "staticstic.h"

#include "include/header.h"
#include "include/index_wrapper.h"
#include "include/virtual_db_mock.h"

#include "rocksdb/perf_context.h"

namespace rocksdb {
extern thread_local PerfContext perf_context;
extern __thread PerfLevel perf_level;  // = kEnableCount;
}  // namespace rocksdb
thread_local uint64_t last_perf_count = 0;

namespace wwsearch {

static int kThreadStop = false;
static constexpr int kFlushLogMaxSize = 20 * 1024 * 1024;

// * merge operator
// * multi instance
// * batch write
const char *BenchIndex::Description =
    "-d [data dir] -p [instance of db] -n [threads per db] -e [debug] \n"
    "\t\t-f [run times per thread] -g [batch num] -h [str attr len] \n "
    "\t\t-i [numeric att num] -j [string attr num] -k [suffix attr num] -l \n"
    "\t\t[max uin num] -o [mode 1:add 2:kv debug] -r [rocks "
    "perf:1=disable,2=count,3=time,4=time and lock] -t [mmseg path] -s [suffix "
    "or not 1/0] -m [mock or not 1/0]";
const char *BenchIndex::Usage = "Benchmark for index ";

void BenchIndex::Run(wwsearch::ArgsHelper &args) {
  printf("doing %s\n", __FUNCTION__);
  uint32_t instance_num = args.UInt('p');

  int thread_num = args.UInt('n');
  assert(thread_num > 0);

  BenchIndexParams job_params;
  job_params.debug_ = args.UInt64('e');
  job_params.run_times_ = args.UInt64('f');
  job_params.batch_num_ = args.UInt64('g');
  job_params.str_len_ = args.UInt64('h');
  job_params.nummeric_attr_num_ = args.UInt64('i');
  job_params.string_attr_num_ = args.UInt64('j');
  job_params.suffix_attr_num_ = args.UInt64('k');
  job_params.max_uin_num_ = args.UInt64('l');
  job_params.index_type = args.UInt64('o');
  job_params.perf_rocks = args.UInt64('r');
  job_params.mock = args.UInt64('m');

  std::vector<DefaultIndexWrapper *> indexers;
  std::vector<std::thread *> new_threads;
  std::vector<std::thread *> staticstic_threads;
  std::vector<Staticstic *> staticstics;
  std::vector<RandomCreater *> random_creaters;
  unsigned int begin_seed = time(NULL);

  char buffer[256];
  job_params.Print();
  for (size_t i = 0; i < instance_num; i++) {
    snprintf(buffer, sizeof(buffer), "%s/db_%d", args.String('d').c_str(), i);
    DefaultIndexWrapper *indexer = new DefaultIndexWrapper();
    indexer->DBParams().path = buffer;
    indexer->DBParams().mmseg_instance_num_ = thread_num;
    if (args.Have('t')) {
      indexer->DBParams().mmseg_dict_dir_ = args.String('t');
    } else {
      indexer->DBParams().mmseg_dict_dir_ = "./";
    }
    indexer->Config().SetLogLevel(wwsearch::kSearchLogLevelError);

    if (job_params.mock == 1) {
      VirtualDB *vdb = new VirtualDBMock(indexer->codec_);
      indexer->SetVdb(vdb);
    }

    auto search_stuats = indexer->Open();
    if (!search_stuats.OK()) {
      printf("open db fail,ret:%d,msg:%s\n", search_stuats.GetCode(),
             search_stuats.GetState().c_str());
    }
    assert(search_stuats.OK());
    indexers.push_back(indexer);

    snprintf(buffer, sizeof(buffer), "./staticstic_log/bench_index_%d", i);
    Staticstic *staticstic = new Staticstic(buffer, 10, kFlushLogMaxSize);
    staticstics.push_back(staticstic);
    printf("instance [%d],see log [%s]\n", i, buffer);

    for (size_t i = 0; i < thread_num; i++) {
      RandomCreater *random_creater = new RandomCreater();
      random_creater->Init(begin_seed++);
      random_creaters.push_back(random_creater);
      std::thread *new_thread = new std::thread(
          BenchIndex::ThreadRun, std::ref(*(indexers.back())),
          std::ref(*staticstic), random_creater, std::ref(job_params));
      new_threads.push_back(new_thread);
    }

    std::thread *staticstic_thread =
        new std::thread(BenchIndex::PrintStaitic, std::ref(*staticstic));
    staticstic_threads.push_back(staticstic_thread);
  }

  for (size_t i = 0; i < new_threads.size(); i++) {
    new_threads[i]->join();
    delete new_threads[i];
    delete random_creaters[i];
  }
  kThreadStop = true;
  for (size_t i = 0; i < staticstic_threads.size(); i++) {
    staticstic_threads[i]->join();
    delete staticstic_threads[i];
    delete staticstics[i];
  }
  for (auto indexer : indexers) {
    delete indexer;
  }
  SearchLogDebug("Run finish");
  return;
}

static void InitStringField(wwsearch::IndexField *field, uint16_t field_id,
                            const std::string &word, bool suffix = false) {
  ::wwsearch::IndexFieldFlag flag;
  flag.SetDocValue();
  flag.SetStoredField();
  flag.SetTokenize();
  flag.SetInvertIndex();
  flag.SetInvertIndex();
  if (suffix) {
    flag.SetSuffixBuild();
  }
  field->SetMeta(field_id, flag);
  field->SetString(word);
}

static void InitUint32Field(wwsearch::IndexField *field, uint16_t field_id,
                            uint32_t value) {
  ::wwsearch::IndexFieldFlag flag;
  flag.SetDocValue();
  flag.SetStoredField();
  flag.SetTokenize();
  flag.SetInvertIndex();
  field->SetMeta(field_id, flag);
  field->SetUint32(value);
}

static void InitUint64Field(wwsearch::IndexField *field, uint16_t field_id,
                            uint64_t value) {
  ::wwsearch::IndexFieldFlag flag;
  flag.SetDocValue();
  flag.SetStoredField();
  flag.SetTokenize();
  flag.SetInvertIndex();
  field->SetMeta(field_id, flag);
  field->SetUint64(value);
}

void BenchIndex::CollectRocksDBPerf(Staticstic *staticstic, uint64_t count,
                                    bool quit) {
  last_perf_count += count;
  if (last_perf_count == 0) return;

  // printf("%s\n",rocksdb::perf_context.ToString(true).c_str());
  if (last_perf_count >= 100 || quit) {
// NOTE: real is nana seconds
#define ROCKS_STAT(metric)                                            \
  {                                                                   \
    timeval begin, end;                                               \
    begin.tv_sec = 0;                                                 \
    begin.tv_usec = 0;                                                \
    uint64_t cost = (rocksdb::perf_context.metric) / last_perf_count; \
    end.tv_sec = (cost / 1000000);                                    \
    end.tv_usec = (cost % 1000000);                                   \
    if (cost != 0) staticstic->AddStat(#metric, 0, begin, &end);      \
  }

    ROCKS_STAT(user_key_comparison_count);
    ROCKS_STAT(block_cache_hit_count);
    ROCKS_STAT(block_read_count);
    ROCKS_STAT(block_read_byte);
    ROCKS_STAT(block_read_time);
    ROCKS_STAT(block_checksum_time);
    ROCKS_STAT(block_decompress_time);

    ROCKS_STAT(get_read_bytes);
    ROCKS_STAT(multiget_read_bytes);
    ROCKS_STAT(iter_read_bytes);

    ROCKS_STAT(internal_key_skipped_count);
    ROCKS_STAT(internal_delete_skipped_count);
    ROCKS_STAT(internal_recent_skipped_count);

    ROCKS_STAT(internal_merge_count);

    ROCKS_STAT(get_snapshot_time);
    ROCKS_STAT(get_from_memtable_time);
    ROCKS_STAT(get_from_memtable_count);
    ROCKS_STAT(get_post_process_time);
    ROCKS_STAT(get_from_output_files_time);
    ROCKS_STAT(seek_on_memtable_time);
    ROCKS_STAT(seek_on_memtable_count);
    ROCKS_STAT(next_on_memtable_count);
    ROCKS_STAT(prev_on_memtable_count);
    ROCKS_STAT(seek_child_seek_time);
    ROCKS_STAT(seek_child_seek_count);
    ROCKS_STAT(seek_min_heap_time);
    ROCKS_STAT(seek_max_heap_time);
    ROCKS_STAT(seek_internal_seek_time);
    ROCKS_STAT(find_next_user_entry_time);

    ROCKS_STAT(write_wal_time);
    ROCKS_STAT(write_memtable_time);
    ROCKS_STAT(write_delay_time);
    ROCKS_STAT(write_scheduling_flushes_compactions_time);
    ROCKS_STAT(write_pre_and_post_process_time);
    ROCKS_STAT(write_thread_wait_nanos);
    ROCKS_STAT(db_mutex_lock_nanos);
    ROCKS_STAT(db_condition_wait_nanos);
    ROCKS_STAT(merge_operator_time_nanos);
    ROCKS_STAT(read_index_block_nanos);
    ROCKS_STAT(read_filter_block_nanos);
    ROCKS_STAT(new_table_block_iter_nanos);
    ROCKS_STAT(new_table_iterator_nanos);
    ROCKS_STAT(block_seek_nanos);
    ROCKS_STAT(find_table_nanos);
    ROCKS_STAT(bloom_memtable_hit_count);
    ROCKS_STAT(bloom_memtable_miss_count);
    ROCKS_STAT(bloom_sst_hit_count);
    ROCKS_STAT(bloom_sst_miss_count);

    ROCKS_STAT(key_lock_wait_time);
    ROCKS_STAT(key_lock_wait_count);

    ROCKS_STAT(env_new_sequential_file_nanos);
    ROCKS_STAT(env_new_random_access_file_nanos);
    ROCKS_STAT(env_new_writable_file_nanos);
    ROCKS_STAT(env_reuse_writable_file_nanos);
    ROCKS_STAT(env_new_random_rw_file_nanos);
    ROCKS_STAT(env_new_directory_nanos);
    ROCKS_STAT(env_file_exists_nanos);
    ROCKS_STAT(env_get_children_nanos);
    ROCKS_STAT(env_get_children_file_attributes_nanos);
    ROCKS_STAT(env_delete_file_nanos);
    ROCKS_STAT(env_create_dir_nanos);
    ROCKS_STAT(env_create_dir_if_missing_nanos);
    ROCKS_STAT(env_delete_dir_nanos);
    ROCKS_STAT(env_get_file_size_nanos);
    ROCKS_STAT(env_get_file_modification_time_nanos);
    ROCKS_STAT(env_rename_file_nanos);
    ROCKS_STAT(env_link_file_nanos);
    ROCKS_STAT(env_lock_file_nanos);
    ROCKS_STAT(env_unlock_file_nanos);
    ROCKS_STAT(env_new_logger_nanos);

    rocksdb::perf_context.Reset();
    last_perf_count = 0;
  }
}

void BenchIndex::ThreadRun(DefaultIndexWrapper &wrapper, Staticstic &staticstic,
                           RandomCreater *random_creater,
                           BenchIndexParams &params) {
  // rocksdb
  rocksdb::perf_level = (rocksdb::PerfLevel)(params.perf_rocks);

  char buffer[200];
#define ATTR_SIZE (6)
  const char *format[ATTR_SIZE] = {
      "name_%010u", "pinyin_%010u", "english_%010u", "1%10u", "mail%10u", "%u"};
  uint64_t run_times = params.run_times_;
  assert(params.batch_num_ > 0);
  printf("Start thread\n");
  while (run_times-- > 0 && !kThreadStop) {
    std::vector<wwsearch::DocumentUpdater *> documents;
    for (size_t i = 0; i < params.batch_num_; i++) {
      wwsearch::DocumentUpdater *document_updater =
          new wwsearch::DocumentUpdater();
      wwsearch::Document &document = document_updater->New();

      DocumentID doc_id = random_creater->GetUInt64();
      while (0 == doc_id) {
        random_creater->GetUInt64();
      }
      document.SetID(doc_id);  // start from 1

      uint32_t field_id = 0;
      for (size_t i = 0; i < params.nummeric_attr_num_; i++) {
        InitUint32Field(document.AddField(), field_id++,
                        random_creater->GetUInt32());
      }
      for (size_t i = 0; i < params.string_attr_num_; i++) {
        InitStringField(document.AddField(), field_id++,
                        random_creater->GetString(params.str_len_), false);
      }
      for (size_t i = 0; i < params.suffix_attr_num_; i++) {
        InitStringField(document.AddField(), field_id++,
                        random_creater->GetString(params.str_len_), true);
      }
      /*
      uint32_t v;
      //
      name,pinyin,english,phone,mail,usrid,exattrs,corpid,ppartyid,type,update
      for (int field_id = 0; field_id < ATTR_SIZE; field_id++) {
        wwsearch::IndexFieldFlag flag;
        flag.SetStoredField();
        flag.SetTokenize();
        flag.SetInvertIndex();
        if (field_id == 3) flag.SetSuffixBuild();
        snprintf(buffer, sizeof(buffer), format[field_id],
                 random_creater->GetUInt32());
        auto field = document.AddField();
        field->SetMeta(field_id, flag);
        field->SetString(buffer);
      }
      */
      documents.push_back(document_updater);
    }

    TableID table;
    table.business_type = 0;
    table.partition_set = random_creater->GetUInt32() % params.max_uin_num_;

    wwsearch::SearchTracer tracer;

    timeval begin, end;
    gettimeofday(&begin, NULL);
    bool success = false;
    if (params.index_type == 1) {
      success = wrapper.index_writer_->AddDocuments(table, documents, nullptr,
                                                    &tracer);
    } else if (params.index_type == 2) {
      success = wrapper.index_writer_->AddOrUpdateDocuments(table, documents,
                                                            nullptr, &tracer);
    } else if (params.index_type == 3) {
      success = wrapper.index_writer_->UpdateDocuments(table, documents,
                                                       nullptr, &tracer);
    } else {
      // donothings
      // assert(false);
    }

    gettimeofday(&end, NULL);

    for (auto &du : documents) {
      staticstic.AddStat("BenchIndex", du->Status().GetCode(), begin, &end);
      staticstic.AddStat("OK", success ? 0 : -1, begin, &end);
      staticstic.AddStat("Keys", 0,
                         tracer.Get(wwsearch::TracerType::kRealInsertKeys),
                         begin, &end);
      staticstic.AddStat("Keys", 1,
                         tracer.Get(wwsearch::TracerType::kDocumentCount),
                         begin, &end);

      if (!du->Status().OK() && !du->Status().DocumentExist()) {
        printf("Other error,ret:%d,msg:%s]n", du->Status().GetCode(),
               du->Status().GetState().c_str());
        assert(false);
      }
      delete du;
    }
    printf("release documents run_times %llu ... \n", run_times);
    documents.clear();

    CollectRocksDBPerf(&staticstic, params.batch_num_);
    // rocksdb perf time.
  }
  CollectRocksDBPerf(&staticstic, 0, true);
}

void BenchIndex::PrintStaitic(Staticstic &staticstic) {
  while (!kThreadStop) {
    staticstic.Report();
    sleep(10);
  }
}

}  // namespace wwsearch
