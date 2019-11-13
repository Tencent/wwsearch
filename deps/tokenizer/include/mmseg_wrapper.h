#pragma once

#include <stdint.h>
#include <atomic>
#include <mutex>
#include <set>
#include <string>

#ifdef WIN32
#include "bsd_getopt_win.h"
#else
#include "bsd_getopt.h"
#endif

#include "Segmenter.h"
#include "SegmenterManager.h"
#include "SynonymsDict.h"
#include "ThesaurusDict.h"
#include "UnigramCorpusReader.h"
#include "UnigramDict.h"
#include "csr_utils.h"

namespace mmseg {

class HashLockSupplier {
 public:
  virtual ~HashLockSupplier() {}

  virtual void Lock(uint32_t hash) = 0;

  virtual void unlock(uint32_t hash) = 0;
};

class ThreadHashLock : public HashLockSupplier {
 private:
  std::mutex *locks_;
  uint32_t locks_num_;

 public:
  ThreadHashLock(uint32_t locks_num) {
    this->locks_num_ = locks_num;
    this->locks_ = new std::mutex[locks_num_];
  }
  virtual ~ThreadHashLock() {
    if (nullptr != locks_) {
      delete locks_;
      locks_ = nullptr;
    }
  }

  virtual void Lock(uint32_t hash) { locks_[hash % locks_num_].lock(); }

  virtual void unlock(uint32_t hash) { locks_[hash % locks_num_].unlock(); }

 private:
};

class MMSEGWrapper {
 private:
  std::vector<css::SegmenterManager *> managers_;
  size_t manager_num_;
  HashLockSupplier *hash_lock_;
  std::atomic<std::uint64_t> seq_;

 public:
  MMSEGWrapper() : hash_lock_(NULL) {}

  virtual ~MMSEGWrapper() {
    for (auto seg : managers_) delete seg;
    if (hash_lock_) delete hash_lock_;
  }

  // Note:
  // file: dict_path/uni.lib must exist
  // file: dict_path/mmseg.ini must exist
  void Init(const char *dict_path, size_t segmenter_num = 5000) {
    hash_lock_ = new ThreadHashLock(segmenter_num);
    manager_num_ = segmenter_num;
    assert(manager_num_ > 0);
    for (size_t i = 0; i < manager_num_; i++) {
      css::SegmenterManager *seg = new css::SegmenterManager();
      assert(0 == seg->init(dict_path));
      managers_.push_back(seg);
    }
  }

  static MMSEGWrapper *Instance() {
    static MMSEGWrapper segment;
    return &segment;
  }

  virtual bool BuildTerms(const char *buffer, size_t buffer_size,
                          std::set<std::string> &terms);

 private:
};
}  // namespace mmseg
