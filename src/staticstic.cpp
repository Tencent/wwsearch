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

#include "staticstic.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <sstream>

namespace wwsearch {

namespace {
const int kBufferLen{512};
}

// Add one metric.
// [stat_name] + [result] -> [count]
// If end == NULL, end will be set to current time.
void Staticstic::AddStat(const std::string& stat_name, int result,
                         int64_t count, const struct ::timeval& begin,
                         struct ::timeval* end) {
  struct ::timeval internal_end;
  if (NULL == end) {
    gettimeofday(&internal_end, NULL);
  } else {
    std::memcpy(&internal_end, end, sizeof(struct ::timeval));
  }
  AddStat(stat_name, result, count,
          Time::CalculateConsumeTimeUs(begin, internal_end));
}

// Wrapper
void Staticstic::AddStat(const std::string& stat_name, int result,
                         const struct ::timeval& begin, struct ::timeval* end) {
  AddStat(stat_name, result, 1, begin, end);
}

// Innser api
void Staticstic::AddStat(const std::string& stat_name, int result,
                         int64_t count, uint64_t consume_us) {
  StatNameEntityListPtr stat_name_entity_list;
  std::lock_guard<std::mutex> guard(mutex_);
  stat_name_entity_list = stat_name_entity_list_;
  auto iter = stat_name_entity_list->find(stat_name);
  if (iter == stat_name_entity_list->end()) {
    ResultEntity result_entity{{result, StatEntity(count, consume_us)}};
    stat_name_entity_list->emplace(stat_name, std::move(result_entity));
    return;
  }
  ResultEntity& result_entity = iter->second;
  auto iter2 = result_entity.find(result);
  if (iter2 == result_entity.end()) {
    result_entity.emplace(result, StatEntity(count, consume_us));
    return;
  }
  iter2->second.Add(count, consume_us);
}

// print to file
int Staticstic::Reopen() {
  struct stat s;
  std::memset(&s, 0, sizeof(struct stat));
  std::string inuse_log_file_name = log_file_base_ + std::string({".log"});

  if (fd_ == NULL) {
    fd_ = fopen(inuse_log_file_name.c_str(), "a+");
  }

  if (fd_ == NULL || stat(inuse_log_file_name.c_str(), &s) < 0) {
    printf("fd couldn't open or stat failed, inuse_log_file_name %s\n",
           inuse_log_file_name.c_str());
    fd_ = NULL;
    return -1;
  }

  return s.st_size;
}

// switch another file.
int Staticstic::SwitchFile() {
  int i = 0;
  int log_size = Reopen();
  if (log_size < 0) {
    printf("Reopen faield!\n");
    return -2;
  }

  if (log_size < log_max_size_) {
    return 0;
  }

  std::string log_file_name =
      log_file_base_ + std::to_string(log_max_num_ - 1) + std::string(".log");
  if (access(log_file_name.c_str(), F_OK) == 0) {
    if (remove(log_file_name.c_str()) < 0) {
      return -1;
    }
  }

  for (i = log_max_num_ - 2; i >= 0; i--) {
    if (i == 0) {
      log_file_name = log_file_base_ + std::string(".log");
    } else {
      log_file_name = log_file_base_ + std::to_string(i) + std::string(".log");
    }
    if (access(log_file_name.c_str(), F_OK) == 0) {
      std::string new_log_file_name =
          log_file_base_ + std::to_string(i + 1) + std::string(".log");
      if (rename(log_file_name.c_str(), new_log_file_name.c_str()) < 0) {
        return -1;
      }
    }
  }

  if (fd_ != NULL) {
    fclose(fd_);
    fd_ = NULL;
  }
  log_size = Reopen();
  if (log_size < 0) {
    printf("Reopen faield!\n");
    return -2;
  }
  return 0;
}

// write format
void Staticstic::Write(const char* format, ...) {
  va_list ap;
  va_start(ap, format);

  vfprintf(fd_, format, ap);
  va_end(ap);
  fflush(fd_);
}

// print header
std::string Staticstic::GetDateString() {
  char time_str[kBufferLen]{0};
  timeval tval;
  gettimeofday(&tval, NULL);
  struct tm curr;
  curr = *localtime(&tval.tv_sec);

  snprintf(time_str, kBufferLen,
           "\n===============Staticstic in %ds, %04d-%02d-%02d "
           "%02d:%02d:%02d.%03d=====================\n",
           stat_total_time_us_ / (1000 * 1000), curr.tm_year + 1900,
           curr.tm_mon + 1, curr.tm_mday, curr.tm_hour, curr.tm_min,
           curr.tm_sec, (int)tval.tv_usec);
  return std::string(time_str);
}

// print header
std::string Staticstic::BuildHeader() {
  char header[kBufferLen]{0};
  snprintf(header, kBufferLen, "%32s|%8s|%16s|%16s|%9s|%9s|%9s|%9s|%9s|%9s|\n",
           "StatName", "RESULT", "TOTAL", "SUMVAL", "AVG(ms)", "MAX(ms)",
           "MIN(ms)", ">%50ms", ">%80ms", ">%90%ms");
  return std::string(header);
}

// print body
std::string Staticstic::BuildBody(
    const StatNameEntityListPtr& stat_name_entity_list) {
  std::ostringstream o;
  for (auto& stat_name_entity : *stat_name_entity_list) {
    for (auto& result_entity : stat_name_entity.second) {
      auto& entity = result_entity.second;
      uint64_t consume_us_50 = 0;
      uint64_t consume_us_80 = 0;
      uint64_t consume_us_90 = 0;

      std::tie(consume_us_50, consume_us_80, consume_us_90) =
          entity.ConsumeUs();

      char body[kBufferLen]{0};
      snprintf(body, kBufferLen,
               "%32s|%8d|%16llu|%16llu|%9.3f|%9.3f|%9.3f|%9.3f|%9.3f|"
               "%9.3f|\n",
               stat_name_entity.first.c_str(), result_entity.first,
               entity.InvokeCount(), entity.Count(),
               entity.AvgConsumeUs() / 1000.0, entity.MaxConsumeUs() / 1000.0,
               entity.MinConsumeUs() / 1000.0, consume_us_50 / 1000.0,
               consume_us_80 / 1000.0, consume_us_90 / 1000.0);
      o << std::string(body);
    }
  }
  o << std::string(
      {"-----------------------------------------------------------------------"
       "-----------------\n\n"});
  return o.str();
}

// Print statistic info to file.
void Staticstic::Report() {
  StatNameEntityListPtr stat_name_entity_list(new StatNameEntityList);
  {
    std::lock_guard<std::mutex> guard(mutex_);
    stat_name_entity_list_.swap(stat_name_entity_list);
    uint64_t now_us = Time::NowMicros();
    stat_total_time_us_ = now_us - last_stat_time_us_;
    last_stat_time_us_ = now_us;
  }
  if (0 != SwitchFile()) {
    printf("SwitchFile faild!\n");
    return;
  }
  if (fd_ == NULL) {
    printf("Couldn't open fd!\n");
    return;
  }
  std::ostringstream o;
  o << GetDateString();
  o << BuildHeader();
  o << BuildBody(stat_name_entity_list);
  Write("%s\n", o.str().c_str());
  // printf("%s\n", o.str().c_str());
  o.clear();
}

}  // namespace wwsearch
