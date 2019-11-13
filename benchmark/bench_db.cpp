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

#include "bench_db.h"
#include "random_creater.h"

#include "include/header.h"
#include "include/index_wrapper.h"

// Bench design:
//	-d data dir,example /data1/test/db_0
//  -p intance of db
//  -n thread per instance
//  -t run times per instance
//  -m mode,merge or put?
namespace wwsearch {

static int kThreadStop = false;

const char *BenchDB::Description =
    "-d [data dir] -p [instance of db] -n [threads per db] -t [run times per "
    "instance] -m [0:not merge 1:merge]";

const char *BenchDB::Usage = "Benchmark for db put";
static constexpr int kFlushLogMaxSize = 20 * 1024 * 1024;

void BenchDB::Run(wwsearch::ArgsHelper &args) {
  printf("doing %s\n", __FUNCTION__);
  uint32_t instance_num = args.UInt('p');
  uint32_t mode = args.UInt('m');

  int thread_num = args.UInt('n');
  assert(thread_num > 0);
  uint64_t run_times = args.UInt64('t');

  std::vector<DefaultIndexWrapper *> indexers;
  std::vector<std::thread *> new_threads;
  std::vector<std::thread *> staticstic_threads;
  std::vector<RandomCreater *> random_creaters;
  unsigned int begin_seed = time(NULL);

  printf("mode=%d\n", mode);

  char buffer[256];
  for (size_t i = 0; i < instance_num; i++) {
    snprintf(buffer, sizeof(buffer), "%s/db_%d", args.String('d').c_str(), i);
    DefaultIndexWrapper *indexer = new DefaultIndexWrapper();
    indexer->DBParams().path = buffer;
    auto search_stuats = indexer->Open();
    if (!search_stuats.OK()) {
      printf("open db fail,ret:%d,msg:%s\n", search_stuats.GetCode(),
             search_stuats.GetState().c_str());
    }
    assert(search_stuats.OK());
    indexers.push_back(indexer);

    snprintf(buffer, sizeof(buffer), "./staticstic_log/bench_db_%d", i);
    Staticstic *staticstic = new Staticstic(buffer, 10, kFlushLogMaxSize);
    printf("instance [%d],see log [%s]\n", i, buffer);

    for (size_t i = 0; i < thread_num; i++) {
      RandomCreater *random_creater = new RandomCreater();
      random_creater->Init(begin_seed++);
      random_creaters.push_back(random_creater);
      std::thread *new_thread = new std::thread(
          BenchDB::ThreadRun, std::ref(*(indexers.back())),
          run_times / thread_num, std::ref(*staticstic), random_creater, mode);
      new_threads.push_back(new_thread);
    }

    std::thread *staticstic_thread =
        new std::thread(BenchDB::PrintStaitic, std::ref(*staticstic));
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
  }
  SearchLogDebug("Run finish");
  return;
}

void BenchDB::ThreadRun(DefaultIndexWrapper &wrapper, uint64_t run_times,
                        Staticstic &staticstic, RandomCreater *random_creater,
                        int mode) {
  char buffer[200];
#define ATTR_SIZE (6)
  const char *format[ATTR_SIZE] = {
      "name_%010u", "pinyin_%010u", "english_%010u", "1%10u", "mail%10u", "%u"};
  while (run_times-- > 0 && !kThreadStop) {
    wwsearch::Document document;
    DocumentID doc_id = random_creater->GetUInt64();
    if (0 == doc_id) doc_id = 1;
    document.SetID(doc_id);  // start from 1

    uint32_t v;
    for (int field_id = 0; field_id < ATTR_SIZE; field_id++) {
      wwsearch::IndexFieldFlag flag;
      flag.SetStoredField();
      flag.SetTokenize();
      if (field_id == 3) flag.SetSuffixBuild();
      snprintf(buffer, sizeof(buffer), format[field_id],
               random_creater->GetUInt32());
      auto field = document.AddField();
      field->SetMeta(field_id, flag);
      field->SetString(buffer);
    }

    std::string str;
    assert(document.SerializeToBytes(str, 0));

    SearchStatus status;
    auto db = wrapper.vdb_;
    auto buffer = db->NewWriteBuffer(nullptr);

    if (mode == 0) {
      status =
          buffer->Put(kStoredFieldColumn, random_creater->GetString(10), str);
    } else {
      status =
          buffer->Merge(kStoredFieldColumn, random_creater->GetString(10), str);
    }
    if (!status.OK()) {
      printf("mode:%d,buffer->put/merge ret:%d,msg:%s\n", mode,
             status.GetCode(), status.GetState().c_str());
    }
    assert(status.OK());

    timeval begin, end;
    gettimeofday(&begin, NULL);
    status = db->FlushBuffer(buffer);
    gettimeofday(&end, NULL);
    if (!status.OK()) {
      printf("mode:%d,flush ret:%d,msg:%s\n", mode, status.GetCode(),
             status.GetState().c_str());
    }
    staticstic.AddStat("BenchDB", status.GetCode(), begin, &end);
    db->ReleaseWriteBuffer(buffer);
  }
}

void BenchDB::PrintStaitic(Staticstic &staticstic) {
  while (!kThreadStop) {
    staticstic.Report();
    sleep(10);
  }
}

}  // namespace wwsearch
