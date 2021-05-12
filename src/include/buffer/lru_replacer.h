//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  /*
  Remove the object that was accessed the least recently compared to all the elements being tracked by the Replacer,
  store its contents in the output parameter and return True. If the Replacer is empty return False.
  */
  bool Victim(frame_id_t *frame_id) override;

  /*
  This method should be called after a page is pinned to a frame in the BufferPoolManager. It should remove the frame
  containing the pinned page from the LRUReplacer.
  */
  void Pin(frame_id_t frame_id) override;

  /*
  This method should be called when the pin_count of a page becomes 0. This method should add the frame containing the
  unpinned page to the LRUReplacer.
  */
  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!

  std::mutex mtx_;
  struct Node;
  using node_t = Node *;
  struct Node {
    frame_id_t id_;
    node_t prev_;
    node_t succ_;
    explicit Node(frame_id_t v) : id_(v), prev_(nullptr), succ_(nullptr) {}
  };
  Node head_;
  Node tail_;

  int capacity_;

  int size_;

  std::unordered_map<frame_id_t, node_t> hash;

  node_t remove(node_t node) {
    node->succ_->prev_ = node->prev_;
    node->prev_->succ_ = node->succ_;
    return node;
  }

  node_t drop(node_t node) {
    auto tmp = remove(node);
    --size_;
    hash.erase(tmp->id_);
    return tmp;
  }

  void insert(node_t prev, node_t node) {
    node->succ_ = prev->succ_;
    node->prev_ = prev;
    node->prev_->succ_ = node;
    node->succ_->prev_ = node;
  }

  void insert2head(node_t node) {
    node->succ_ = head_.succ_;
    node->prev_ = &head_;
    node->prev_->succ_ = node;
    node->succ_->prev_ = node;
  }

  void addNew(frame_id_t frame_id) {
    if (hash.count(frame_id) == 0) {
      if (size_ == capacity_) {
        auto tmp = drop(tail_.prev_);
        delete tmp;
      }
      auto new_node = new Node(frame_id);
      insert2head(new_node);
      hash[new_node->id_] = new_node;
      ++size_;
    }
  }
};

}  // namespace bustub
