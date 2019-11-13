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

#include "header.h"
#include "stat_collector.h"

namespace wwsearch {

enum class TracerType {
  kIndexWriterGetCount,
  kIndexWriterGetKvCount,
  kIndexWriterGetKvSizeBytes,
  kIndexWriterGetConsumeTimeUs,

  kDocumentWriterPutCount,
  kDocumentWriterPutKvCount,
  kDocumentWriterPutKvSizeBytes,
  kDocumentWriterPutConsumeTimeUs,

  kDocumentCount,
  kRealInsertKeys,

  kGetInvertedTableSubQueryCount,
  kGetInvertedTableSubQueryConsumeUs,

  kGetDocvalueCountInCollector,
  kGetDocvalueConsumeUsInCollector,

  kFilterHitCountInCollector,
  kFilterConsumeUsInCollector,

  kExceedInnerPurgeDocsTotalLimitCount,

  kGetStoredocCountInCollector,
  kGetStoredocConsumeUsInCollector,

  kPostScorerConsumeUsInCollector,
  kPostScorerDocCountInCollector,
};

/* SearchTracer aims to trace all time consume and staticstic count
 * in wwsearch internal. This should work for each request for user.
 * So must notice this is not support for multi-threads, only work
 * for single thread.
 */
class SearchTracer {
 public:
  SearchTracer() {}
  virtual ~SearchTracer() {}

  SearchTracer(const SearchTracer&) = delete;
  SearchTracer& operator=(const SearchTracer&) = delete;

  void Set(TracerType type, uint64_t cnt) { tracer_cnt_list_[type] = cnt; }

  void Add(TracerType type, uint64_t cnt) {
    auto iter = tracer_cnt_list_.find(type);
    if (iter == tracer_cnt_list_.end()) {
      tracer_cnt_list_[type] = cnt;
      return;
    }
    iter->second += cnt;
  }

  uint64_t Get(TracerType type) { return tracer_cnt_list_[type]; }

  void Clear() { tracer_cnt_list_.clear(); }

 private:
  std::map<TracerType, uint64_t> tracer_cnt_list_;
};

class TimeCostCounter {
 private:
  uint64_t start_;
  uint64_t end_;
  uint64_t total_;

 public:
  TimeCostCounter() : start_(0), end_(0), total_(0) {}
  virtual ~TimeCostCounter() {}

  void Start() {
    total_ += (end_ - start_);
    start_ = Time::NowNanos();
    end_ = start_;
  }

  void Stop() { end_ = Time::NowNanos(); }

  uint64_t CostNanos() {
    Stop();
    return (total_ + (end_ - start_));
  }

  uint64_t CostUs() { return CostNanos() / 1000; }

 private:
};

// Stop the timer and update the metric
#define WWSEARCH_PERF_TIMER_STOP(metric) perf_step_timer_##metric.Stop();

#define WWSEARCH_PERF_TIMER_START(metric) perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define WWSEARCH_PERF_TIMER_GUARD(metric)                           \
  TimeCostCounter perf_step_timer_##metric(&(perf_context.metric)); \
  perf_step_timer_##metric.Start();

}  // namespace wwsearch
