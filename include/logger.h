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

#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <sys/prctl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

const uint32_t kSearchLogBufSize = 8192;
extern __thread char g_search_log_buffer[kSearchLogBufSize];
extern uint32_t g_search_log_level;
extern bool g_search_use_log;
extern uint32_t g_search_use_console;

namespace wwsearch {
class Logger;
}
extern wwsearch::Logger* g_logger;

namespace wwsearch {

/* Notice : Logger base class. Default implement to printf in console.
 * Support users to implement yourself logger by inherit from Logger class.
 */
enum SearchLogLevel {
  kSearchLogLevelZero = 0,
  kSearchLogLevelFatal = 0,
  kSearchLogLevelImpt = 0,
  kSearchLogLevelError = 1,
  kSearchLogLevelWarn = 2,
  kSearchLogLevelInfo = 3,
  kSearchLogLevelDebug = 4,
};

class Logger {
 public:
  // Set Log leel
  static void SetLogLevel(uint32_t level, bool use_search_log = true) {
    g_search_log_level = level;
    g_search_use_log = use_search_log;
  }
  // Set outer logger implement
  static void SetLogger(Logger* logger,
                        SearchLogLevel level = kSearchLogLevelError) {
    if (NULL == g_logger) {
      g_logger = logger;
    }
    Logger::SetLogLevel(level, true);
  }
  // Write Info Log
  virtual void WriteInfo(const char* log_buffer) = 0;
  // Write Error Log
  virtual void WriteErr(const char* log_buffer) = 0;
  // Write Debug Log
  virtual void WriteDebug(const char* log_buffer) = 0;
};

// Default logger
class DefaultLoggerImpl : public Logger {
 public:
  DefaultLoggerImpl();
  ~DefaultLoggerImpl() {}

 public:
  void WriteInfo(const char* log_buffer);
  void WriteErr(const char* log_buffer);
  void WriteDebug(const char* log_buffer);

 private:
  void WriteLog(const char* pcBuffer);
};

#define SimpleSearchLog(fmt, args...)                                         \
  do {                                                                        \
    assert(g_logger != NULL);                                                 \
    int __iLen =                                                              \
        snprintf(g_search_log_buffer, kSearchLogBufSize, " %s:%s:%u " fmt "", \
                 __FILE__, __FUNCTION__, __LINE__, ##args);                   \
    if (__iLen > int(kSearchLogBufSize)) __iLen = kSearchLogBufSize;          \
  } while (0);

#define SearchLog(__level, fmt, args...)                                       \
  do {                                                                         \
    assert(g_logger != NULL);                                                  \
    struct timeval __now_tv;                                                   \
    gettimeofday(&__now_tv, NULL);                                             \
    const time_t __seconds = __now_tv.tv_sec;                                  \
    struct tm __t;                                                             \
    localtime_r(&__seconds, &__t);                                             \
    int __iLen =                                                               \
        snprintf(g_search_log_buffer, kSearchLogBufSize,                       \
                 "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s %s:%s:%u " fmt "",     \
                 __t.tm_year + 1900, __t.tm_mon + 1, __t.tm_mday, __t.tm_hour, \
                 __t.tm_min, __t.tm_sec, static_cast<int>(__now_tv.tv_usec),   \
                 __level, __FILE__, __FUNCTION__, __LINE__, ##args);           \
    if (__iLen > int(kSearchLogBufSize)) __iLen = kSearchLogBufSize;           \
  } while (0);

#define EnableDebugLog (g_search_log_level >= wwsearch::kSearchLogLevelDebug)

#define SearchLogDebug(fmt, args...)                                \
  do {                                                              \
    if (g_search_log_level < wwsearch::kSearchLogLevelDebug) break; \
    if (g_search_use_log) {                                         \
      SearchLog("Debug", fmt, ##args);                              \
    } else {                                                        \
      SimpleSearchLog(fmt, ##args);                                 \
    }                                                               \
    g_logger->WriteDebug(g_search_log_buffer);                      \
  } while (0);

#define SearchLogInfo(fmt, args...)                                \
  do {                                                             \
    if (g_search_log_level < wwsearch::kSearchLogLevelInfo) break; \
    if (g_search_use_log) {                                        \
      SearchLog("Info", fmt, ##args);                              \
    } else {                                                       \
      SimpleSearchLog(fmt, ##args);                                \
    }                                                              \
    g_logger->WriteInfo(g_search_log_buffer);                      \
  } while (0);

#define SearchLogWarn(fmt, args...)                                \
  do {                                                             \
    if (g_search_log_level < wwsearch::kSearchLogLevelWarn) break; \
    if (g_search_use_log) {                                        \
      SearchLog("Warn", fmt, ##args);                              \
    } else {                                                       \
      SimpleSearchLog(fmt, ##args);                                \
    }                                                              \
    g_logger->WriteErr(g_search_log_buffer);                       \
  } while (0);

#define SearchLogError(fmt, args...)                                \
  do {                                                              \
    if (g_search_log_level < wwsearch::kSearchLogLevelError) break; \
    if (g_search_use_log) {                                         \
      SearchLog("Error", fmt, ##args);                              \
    } else {                                                        \
      SimpleSearchLog(fmt, ##args);                                 \
    }                                                               \
    g_logger->WriteErr(g_search_log_buffer);                        \
  } while (0);

#define SearchLogImpt(fmt, args...)                                \
  do {                                                             \
    if (g_search_log_level < wwsearch::kSearchLogLevelImpt) break; \
    if (g_search_use_log) {                                        \
      SearchLog("Impt", fmt, ##args);                              \
    } else {                                                       \
      SimpleSearchLog(fmt, ##args);                                \
    }                                                              \
    g_logger->WriteErr(g_search_log_buffer);                       \
  } while (0);

#define SearchLogFatal(fmt, args...)         \
  do {                                       \
    wwsearch::OSS::ReportFatalErr();         \
    if (g_search_use_log) {                  \
      SearchLog("Fatal", fmt, ##args);       \
    } else {                                 \
      SimpleSearchLog(fmt, ##args);          \
    }                                        \
    g_logger->WriteErr(g_search_log_buffer); \
  } while (0);

#define SearchLogZero(fmt, args...)          \
  do {                                       \
    if (g_search_use_log) {                  \
      SearchLog("Zero", fmt, ##args);        \
    } else {                                 \
      SimpleSearchLog(fmt, ##args);          \
    }                                        \
    g_logger->WriteErr(g_search_log_buffer); \
  } while (0);

}  // namespace wwsearch
