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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (frames_.empty()) {
    return false;
  }

  *frame_id = frames_.begin()->first;
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
      }
    } else {
      if (cur_distance > max_distance && !frame.second.is_cached_) {
        max_distance = cur_distance;
        *frame_id = frame.first;
      }
    }
  }

  if (!frames_.find(*frame_id)->second.is_cached_) {
    no_cached_size_--;
  }
  frames_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
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
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (auto search = frames_.find(frame_id); search != frames_.end()) {
    if (search->second.is_evi_ ^ set_evictable) {
      curr_size_ += set_evictable ? 1 : -1;
      search->second.is_evi_ = set_evictable;
    }
  } else {
    throw "invalid frame id!";
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (auto search = frames_.find(frame_id); search != frames_.end()) {
    if (!search->second.is_evi_) {
      throw "non evictable frame!";
    }
    frames_.erase(frame_id);
    no_cached_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
