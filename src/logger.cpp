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

#include "logger.h"
#include <stdio.h>

__thread char g_search_log_buffer[kSearchLogBufSize];
uint32_t g_search_log_level;
bool g_search_use_log;
uint32_t g_search_use_console;
wwsearch::Logger* g_logger = NULL;

namespace wwsearch {
DefaultLoggerImpl::DefaultLoggerImpl() {}

void DefaultLoggerImpl::WriteInfo(const char* log_buffer) {
  WriteLog(log_buffer);
}
void DefaultLoggerImpl::WriteErr(const char* log_buffer) {
  WriteLog(log_buffer);
}
void DefaultLoggerImpl::WriteDebug(const char* log_buffer) {
  WriteLog(log_buffer);
}
void DefaultLoggerImpl::WriteLog(const char* log_buffer) {
  printf("%s\n", log_buffer);
}
}  // namespace wwsearch
