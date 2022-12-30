//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  page_id_t new_page_id = AllocatePage();
  *page_id = new_page_id;

  Page *target = nullptr;
  frame_id_t target_frame;
  if (!free_list_.empty()) {
    // find in free list
    target_frame = free_list_.back();
    free_list_.pop_back();

    target = pages_ + target_frame;
    ReadPageToPool(new_page_id, target_frame);
    target->pin_count_++;
    replacer_->RecordAccess(target_frame);
    replacer_->SetEvictable(target_frame, false);

  } else {
    // find in replacable page the pin count is zero.
    if (!replacer_->Evict(&target_frame)) {
      return nullptr;
    }

    target = pages_ + target_frame;
    // write back to disk
    if (target->is_dirty_) {
      disk_manager_->WritePage(target->page_id_, target->data_);
    }

    page_table_->Remove(target->page_id_);

    // replace the page
    ReadPageToPool(new_page_id, target_frame);
    target->pin_count_++;
    replacer_->RecordAccess(target_frame);
    replacer_->SetEvictable(target_frame, false);
  }

  return target;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  Page *target = nullptr;
  frame_id_t target_frame;

  // can find
  if (page_table_->Find(page_id, target_frame)) {
    target = pages_ + target_frame;
    target->pin_count_++;
    replacer_->RecordAccess(target_frame);
    replacer_->SetEvictable(target_frame, false);
    return target;
  }

  // can not find
  if (!free_list_.empty()) {
    // find in free list
    target_frame = free_list_.back();
    free_list_.pop_back();

    target = pages_ + target_frame;
    ReadPageToPool(page_id, target_frame);
    target->pin_count_++;
    replacer_->RecordAccess(target_frame);
    replacer_->SetEvictable(target_frame, false);

  } else {
    // find in replacable page the pin count is zero.
    if (!replacer_->Evict(&target_frame)) {
      return nullptr;
    }

    target = pages_ + target_frame;
    // write back to disk
    if (target->is_dirty_) {
      disk_manager_->WritePage(target->page_id_, target->data_);
      target->is_dirty_ = false;
    }

    page_table_->Remove(target->page_id_);

    // replace the page just cover!!!
    ReadPageToPool(page_id, target_frame);
    target->pin_count_++;
    replacer_->RecordAccess(target_frame);
    replacer_->SetEvictable(target_frame, false);
  }

  return target;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t target_frame;
  Page *target = nullptr;

  if (page_table_->Find(page_id, target_frame)) {
    target = pages_ + target_frame;
    if (target->pin_count_ <= 0) {
      return false;
    }
  } else {
    return false;
  }

  target->pin_count_--;
  if (target->pin_count_ <= 0) {
    replacer_->SetEvictable(target_frame, true);
  }

  target->is_dirty_ = is_dirty || target->is_dirty_;

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t target_frame;
  Page *target = nullptr;

  if (page_table_->Find(page_id, target_frame)) {
    target = pages_ + target_frame;
  } else {
    return false;
  }

  disk_manager_->WritePage(target->page_id_, target->data_);
  target->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);

  Page *target = pages_;
  for (size_t i = 0; i < pool_size_; ++i) {
    target += i;
    if (target->page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(target->page_id_, target->data_);
      target->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t target_frame;
  Page *target = nullptr;

  if (page_table_->Find(page_id, target_frame)) {
    target = pages_ + target_frame;
    if (target->pin_count_ > 0) {
      return false;
    }
  } else {
    return true;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(target_frame);
  free_list_.push_back(target_frame);

  ResetPage(target);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::ResetPage(Page *page) {
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
}

}  // namespace bustub
