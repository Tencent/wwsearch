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

#include <sys/time.h>
#include <time.h>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include "stat_collector.h"

namespace wwsearch {

class StatEntity {
  friend class StatEntity;

 public:
  explicit StatEntity(int64_t count, uint32_t consume_us) {
    count_.Add(count);
    consume_us_.Add(consume_us);
  }
  ~StatEntity() {}

  StatEntity(const StatEntity& stat_entity) { *this = stat_entity; }

  StatEntity& operator=(const StatEntity& stat_entity) {
    this->count_.Add(stat_entity.count_.Get());
    this->consume_us_.Merge(stat_entity.consume_us_);
  }

  StatEntity(StatEntity&& stat_entity) { *this = std::move(stat_entity); }

  StatEntity& operator=(StatEntity&& stat_entity) {
    this->count_.Add(stat_entity.count_.Get());
    this->consume_us_.Merge(stat_entity.consume_us_);
  }

  void Add(int64_t count, uint64_t consume_us) {
    count_.Add(count);
    consume_us_.Add(consume_us);
  }

  uint64_t InvokeCount() { return consume_us_.num(); }
  uint64_t Count() { return count_.Get(); }
  uint64_t AvgConsumeUs() { return consume_us_.Average(); }
  uint64_t MaxConsumeUs() { return consume_us_.max(); }
  uint64_t MinConsumeUs() { return consume_us_.min(); }
  // 50%, 80%, 90%
  std::tuple<uint64_t, uint64_t, uint64_t> ConsumeUs() {
    return std::make_tuple<uint64_t, uint64_t, uint64_t>(
        consume_us_.Percentile(0.5), consume_us_.Percentile(0.8),
        consume_us_.Percentile(0.9));
  }

 private:
  Counter count_;
  HistogramImpl consume_us_;
};

/*
template <class T>
class TimeGuard {
  friend class TimeGuard;

 public:
  TimeGuard(T&& body, std::function<void(const std::string& , int64_t ,
               uint32_t )>)
      : guard_body_(std::forward<T>(body)), start_time_us_(Time::NowMicros()) {
    on_exit_scope_ = []() {

    };
  }
  TimeGuard(TimeGuard&& time_guard)
      : guard_body_(std::move(time_guard.guard_body_)),
        start_time_us_(time_guard.start_time_us_),
        on_exist_scope_(std::move(time_guard, on_exit_scope_)) {}

~TimeGuard() {

}
 private:
  T guard_body_;
  uint64_t start_time_us_;
  std::function<void()> on_exit_scope_;
};
*/

using ResultEntity = std::map<int, StatEntity>;
using StatNameEntityList = std::map<std::string, ResultEntity>;
using StatNameEntityListPtr = std::shared_ptr<StatNameEntityList>;

static constexpr int kLogMaxSize = 20 * 1024 * 1024;  // 20MB

class Staticstic {
 public:
  static Staticstic* Instance() {
    static Staticstic stat("./stat", 10, kLogMaxSize);
    return &stat;
  }
  explicit Staticstic(const std::string& log_file_base, int log_max_num,
                      int log_max_size)
      : stat_name_entity_list_(new StatNameEntityList),
        log_file_base_(log_file_base),
        log_max_num_(log_max_num),
        log_max_size_(log_max_size),
        stat_total_time_us_(0),
        last_stat_time_us_(Time::NowMicros()),
        timeout_level_ms_({10.0, 100.0, 1000.0}),
        fd_(NULL) {}

  ~Staticstic() {}

  void AddStat(const std::string& stat_name, int result,
               const struct ::timeval& begin, struct ::timeval* end = NULL);
  void AddStat(const std::string& stat_name, int result, int64_t count,
               const struct ::timeval& begin, struct ::timeval* end = NULL);
  void AddStat(const std::string& stat_name, int result, int64_t count,
               uint64_t consume_us);

  // TimeGuard AddStatTimeGuard(const std::string& stat_name, int64_t count);

  void Report();

 private:
  void Write(const char* format, ...);
  int Reopen();
  int SwitchFile();
  std::string GetDateString();
  std::string BuildHeader();
  std::string BuildBody(const StatNameEntityListPtr& stat_name_entity_list);

 private:
  StatNameEntityListPtr stat_name_entity_list_;
  std::mutex mutex_;
  std::string log_file_base_;
  int log_max_num_;
  int log_max_size_;
  int stat_total_time_us_;
  uint64_t last_stat_time_us_;
  std::vector<float> timeout_level_ms_;
  FILE* fd_;
};

}  // namespace wwsearch
