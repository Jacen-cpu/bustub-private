//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (frames_.empty()) {
    latch_.unlock();
    return false;
  }

  bool success = frames_.begin()->second.is_evi_;
  *frame_id = frames_.begin()->first;
  // find the max distance people
  size_t max_distance = 0;
  for (const auto &frame : frames_) {
    if (!frame.second.is_evi_) {
      continue;
    }
    size_t cur_distance = current_timestamp_ - frame.second.last_used_time_;

    if (no_cached_size_ == 0) {
      if (cur_distance > max_distance) {
        max_distance = cur_distance;
        *frame_id = frame.first;
        success = true;
      }
    } else {
      if (cur_distance > max_distance && !frame.second.is_cached_) {
        max_distance = cur_distance;
        *frame_id = frame.first;
        success = true;
      }
    }
  }

  // debug
  LOG_INFO("# Evict the page: %d", *frame_id);

  if (!frames_.find(*frame_id)->second.is_cached_) {
    no_cached_size_--;
  }
  
  if (success) {
    frames_.erase(*frame_id);
    curr_size_--;
  }

  latch_.unlock();
  return success;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  // debug
  LOG_INFO("# Record the page: %d", frame_id);

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw "Invalid frame!";
  }
  // chg the timestamp.
  current_timestamp_++;

  if (auto search = frames_.find(frame_id); search != frames_.end()) {
    if (search->second.ref_time_ + 1 == k_) {
      no_cached_size_ -= 1;
      search->second.ref_time_ = k_;
      search->second.is_cached_ = true;
    } else {
      search->second.ref_time_ += 1;
    }
    search->second.last_used_time_ = current_timestamp_;
  } else {
    // if not found, create a new entry
    FrameAttr new_attr;
    new_attr.ref_time_ = 1;
    new_attr.last_used_time_ = current_timestamp_;
    frames_.insert(std::make_pair(frame_id, new_attr));
    no_cached_size_ += 1;
  }
  
  latch_.unlock(); 
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  //debug
  LOG_INFO("# Set the page $%d", frame_id);

  if (auto search = frames_.find(frame_id); search != frames_.end()) {
    if (search->second.is_evi_ ^ set_evictable) {
      curr_size_ += set_evictable ? 1 : -1;
      search->second.is_evi_ = set_evictable;
    }
  } else {
    throw "invalid frame id!";
  }
  
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();

  if (auto search = frames_.find(frame_id); search != frames_.end()) {
    if (!search->second.is_evi_) {
      throw "non evictable frame!";
    }
    frames_.erase(frame_id);
    no_cached_size_--;
  }

  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { 
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_; 
}
}  // namespace bustub
