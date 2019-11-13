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

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace wwsearch {

template <typename T>
class LockFreeQueue {
 private:
  template <typename D>
  struct node {
    D data;
    node* next;
    explicit node(D&& d) : data(std::forward<D>(d)), next(nullptr) {}
  };
  std::atomic<node<T>*> head_;
  std::atomic<uint64_t> count_;
  std::atomic<uint64_t> times_;

 public:
  // Constructor
  LockFreeQueue() : head_(nullptr), count_(0), times_(0) {}
  ~LockFreeQueue() {
    T data;
    while (Pop(&data) != false)
      ;
  }

  // Push one item without lock
  void Push(T&& data) {
    node<T>* new_node = new node<T>(std::forward<T>(data));

    // put the current value of head into new_node->next
    new_node->next = head_.load(std::memory_order_acquire);

    // now make new_node the new head, but if the head
    // is no longer what's stored in new_node->next
    // (some other thread must have inserted a node just now)
    // then put that new head into new_node->next and try again
    while (!head_.compare_exchange_weak(new_node->next, new_node,
                                        std::memory_order_release,
                                        std::memory_order_relaxed)) {
      new_node->next = head_.load(std::memory_order_acquire);
    }
    count_.fetch_add(1);
  }

  // Pop one item without lock
  bool Pop(T* data) {
    assert(data != nullptr);
    while (1) {
      auto result = head_.load(std::memory_order_acquire);
      if (result == nullptr) {
        return false;
      }
      if (head_.compare_exchange_weak(result, result->next,
                                      std::memory_order_release,
                                      std::memory_order_relaxed)) {
        *data = std::move(result->data);
        count_.fetch_sub(1);
        return true;
      }
    }
  }

  // Check is empty ?
  bool Empty() {
    auto result = head_.load(std::memory_order_acquire);
    if (result == nullptr) {
      return true;
    } else {
      return false;
    }
  }

  // Get queue's szie
  uint64_t Count() { return count_.load(); }
};

// Fix size queue.
template <typename T>
class FixLenLockFreeQueue {
 private:
  // inner api
  constexpr size_t idx(size_t i) const noexcept { return i % capacity_; }

  constexpr size_t turn(size_t i) const noexcept { return i / capacity_; }

  static constexpr size_t kCacheLineSize = 128;

  struct Slot {
    ~Slot() noexcept {
      if (occupy & 1) {
        Destroy();
      }
    }

    template <typename... Args>
    void Construct(Args&&... args) noexcept {
      new (&storage) T(std::forward<Args>(args)...);
    }

    void Destroy() noexcept { reinterpret_cast<T*>(&storage)->~T(); }

    T&& Move() noexcept { return reinterpret_cast<T&&>(storage); }

    // Align to avoid false sharing between adjacent slots
    alignas(kCacheLineSize) std::atomic<size_t> occupy{0};
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
  };

  inline void* align(std::size_t alignment, std::size_t size, void*& ptr,
                     std::size_t& space) {
    std::uintptr_t pn = reinterpret_cast<std::uintptr_t>(ptr);
    std::uintptr_t aligned = (pn + alignment - 1) & -alignment;
    std::size_t padding = aligned - pn;
    if (space < size + padding) return nullptr;
    space -= padding;
    return ptr = reinterpret_cast<void*>(aligned);
  }

 private:
  alignas(kCacheLineSize) std::atomic<size_t> head_;
  alignas(kCacheLineSize) std::atomic<size_t> tail_;
  std::atomic<uint64_t> count_;
  std::atomic<uint64_t> capacity_;
  void* buf_;
  Slot* slots_;

 public:
  FixLenLockFreeQueue(const size_t capacity)
      : head_(0), tail_(0), count_(0), capacity_(capacity) {
    size_t space = capacity * sizeof(Slot) + kCacheLineSize - 1;
    buf_ = std::malloc(space);
    void* buf = buf_;
    slots_ = reinterpret_cast<Slot*>(
        align(kCacheLineSize, capacity * sizeof(Slot), buf, space));
    for (size_t i = 0; i < capacity_; ++i) {
      new (&slots_[i]) Slot();
    }
  }

  ~FixLenLockFreeQueue() {
    for (size_t i = 0; i < capacity_; ++i) {
      slots_[i].~Slot();
    }
    free(buf_);
  }

  // emplace try
  template <typename... Args>
  bool TryEmplace(Args&&... args) noexcept {
    auto head = head_.load(std::memory_order_acquire);
    for (;;) {
      auto& slot = slots_[idx(head)];
      if (turn(head) * 2 == slot.occupy.load(std::memory_order_acquire)) {
        if (head_.compare_exchange_strong(head, head + 1)) {
          slot.Construct(std::forward<Args>(args)...);
          slot.occupy.store(turn(head) * 2 + 1, std::memory_order_release);
          return true;
        }
      } else {
        auto const prev_head = head;
        head = head_.load(std::memory_order_acquire);
        if (head == prev_head) {
          return false;
        }
      }
    };
  }

  // Push one item
  void Push(T&& data) {
    while (false == TryEmplace(std::forward<T>(data)))
      ;
  }

  // Pop one item
  bool Pop(T* data) {
    assert(data != nullptr);
    auto tail = tail_.load(std::memory_order_acquire);
    for (;;) {
      auto& slot = slots_[idx(tail)];
      if (turn(tail) * 2 + 1 == slot.occupy.load(std::memory_order_acquire)) {
        if (tail_.compare_exchange_strong(tail, tail + 1)) {
          *data = slot.Move();
          slot.Destroy();
          slot.occupy.store(turn(tail) * 2 + 2, std::memory_order_release);
          return true;
        }
      } else {
        auto const prev_tail = tail;
        tail = tail_.load(std::memory_order_acquire);
        if (tail == prev_tail) {
          return false;
        }
      }
    };
  }

  bool Empty() { return count_.load() == 0; }

  uint64_t Count() { return count_.load(); }
};
}  // namespace wwsearch
