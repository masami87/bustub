//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::lock_guard<std::mutex> lock_guard{latch_};

  if (this->page_table_.count(page_id)) {
    Page *page = &this->pages_[this->page_table_[page_id]];
    ++page->pin_count_;
    this->replacer_->Pin(this->page_table_[page_id]);
    return page;
  } else {
    frame_id_t frame_id = this->FindReplace();
    if (frame_id == -1) return nullptr;
    page_table_[page_id] = frame_id;
    this->InitNewPage(frame_id, page_id);
    Page *p = this->pages_ + frame_id;
    this->disk_manager_->ReadPage(page_id, p->GetData());
    return p;
  }
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (this->page_table_.count(page_id) == 0) return false;

  frame_id_t frame_id = this->page_table_[page_id];
  Page *page = &this->pages_[frame_id];

  if (page->pin_count_ <= 0) return false;

  if (--page->pin_count_ == 0) {
    this->replacer_->Unpin(frame_id);
  }
  // 重要！！
  page->is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!

  std::lock_guard<std::mutex> lock_guard{latch_};

  if (page_id == INVALID_PAGE_ID || this->page_table_.count(page_id) == 0) return false;

  Page *page = &this->pages_[page_table_[page_id]];

  this->disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock_guard{latch_};

  if (this->IsALLPagesPinned()) return nullptr;

  *page_id = disk_manager_->AllocatePage();

  frame_id_t frame_id = this->FindReplace();

  // init
  this->replacer_->Pin(frame_id);
  this->InitNewPage(frame_id, *page_id);
  this->pages_[frame_id].ResetMemory();
  this->page_table_[*page_id] = frame_id;

  return this->pages_ + frame_id;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock_guard{latch_};

  this->disk_manager_->DeallocatePage(page_id);

  if (this->page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = this->page_table_[page_id];
  Page *page = &this->pages_[frame_id];
  if (page->GetPinCount() > 0) return false;

  this->replacer_->Pin(frame_id);

  this->page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;

  this->free_list_.push_back(frame_id);

  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!

  std::lock_guard<std::mutex> lock_guard{latch_};

  for (auto iter : this->page_table_) {
    Page *page = this->pages_ + iter.second;
    this->disk_manager_->WritePage(iter.first, page->GetData());
    page->is_dirty_ = false;
  }
}

}  // namespace bustub
