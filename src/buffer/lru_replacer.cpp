//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : head_(-1), tail_(-1), capacity_(num_pages), size_(0) {
  head_.succ_ = &tail_;
  tail_.prev_ = &head_;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard{mtx_};
  if (hash.empty()) return false;
  auto tmp = drop(tail_.prev_);
  *frame_id = tmp->id_;
  delete tmp;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard{mtx_};
  if (hash.count(frame_id)) {
    auto tmp = drop(hash.at(frame_id));
    delete tmp;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard{mtx_};
  addNew(frame_id);
}

size_t LRUReplacer::Size() { return size_; }

}  // namespace bustub
