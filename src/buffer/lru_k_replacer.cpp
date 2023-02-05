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
#include <algorithm>
#include <cassert>
#include <deque>
#include <memory>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {
using std::make_pair;

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // LOG_DEBUG("replace size is %zu, k is %zu", num_frames, k);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ <= 0) {
    LOG_DEBUG("Evict Fail!");
    return false;
  }
  /* === evict in history frames === */
  if (!history_frames_.empty()) {
    size_t min_time;
    frame_id_t min_frame_id = -1;
    for (const auto &frame : history_frames_) {
      if (frame.second->CanEvict()) {
        if (min_frame_id == -1) {
          min_time = frame.second->GetOldest();
          min_frame_id = frame.first;
          continue;
        }
        if (frame.second->GetOldest() < min_time) {
          min_time = frame.second->GetOldest();
          min_frame_id = frame.first;
        }
      }
    }
    if (min_frame_id != -1) {
      *frame_id = min_frame_id;
      history_frames_.erase(min_frame_id);
      curr_size_--;
      // LOG_DEBUG("Evict frame %d", min_frame_id);
      return true;
    }
  }
  /* === evict in cache frames === */
  size_t min_time;
  frame_id_t min_frame_id = -1;
  for (const auto &frame : cached_frames_) {
    if (frame.second->CanEvict()) {
      if (min_frame_id == -1) {
        min_time = frame.second->GetOldest();
        min_frame_id = frame.first;
        continue;
      }
      if (frame.second->GetOldest() < min_time) {
        min_time = frame.second->GetOldest();
        min_frame_id = frame.first;
      }
    }
  }
  assert(min_frame_id != -1);
  *frame_id = min_frame_id;
  cached_frames_.erase(min_frame_id);
  curr_size_--;
  // LOG_DEBUG("Evict frame %d", min_frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // LOG_DEBUG("Record frame %d", frame_id);
  current_timestamp_++;
  BUSTUB_ASSERT(CheckFrame(frame_id), "invalid frame_id");
  // find in cached map
  if (auto frame = cached_frames_.find(frame_id); frame != cached_frames_.end()) {
    frame->second->RecordRef(current_timestamp_, true);
    return;
  }
  // find in history map
  if (auto frame = history_frames_.find(frame_id); frame != history_frames_.end()) {
    frame->second->RecordRef(current_timestamp_, false);
    if (frame->second->GetSize() >= k_) {
      cached_frames_.insert(*frame);
      history_frames_.erase(frame);
    }
    return;
  }
  // insert new frame in history map
  auto new_fame = std::make_shared<FrameAttr>(current_timestamp_);
  history_frames_.insert({frame_id, new_fame});
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ASSERT(CheckFrame(frame_id), "invalid frame_id");
  // LOG_DEBUG("Set frame %d, evictable %d", frame_id, set_evictable);
  // find in history
  if (auto frame = history_frames_.find(frame_id); frame != history_frames_.end()) {
    if (set_evictable ^ frame->second->CanEvict()) {
      curr_size_ += set_evictable ? 1 : -1;
    }
    frame->second->SetEvict(set_evictable);
    return;
  }
  // find in cache
  if (auto frame = cached_frames_.find(frame_id); frame != cached_frames_.end()) {
    if (set_evictable ^ frame->second->CanEvict()) {
      curr_size_ += set_evictable ? 1 : -1;
    }
    frame->second->SetEvict(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ASSERT(CheckFrame(frame_id), "invalid frame_id");
  // LOG_DEBUG("Remove frame %d", frame_id);
  if (curr_size_ <= 0) {
    // LOG_DEBUG("Remove Fail!");
    return;
  }
  if (auto frame = history_frames_.find(frame_id); frame != history_frames_.end()) {
    if (!frame->second->CanEvict()) {
      throw Exception("frame can't be evicted");
    }
    history_frames_.erase(frame);
    curr_size_--;
    return;
  }
  // find in cache
  if (auto frame = cached_frames_.find(frame_id); frame != cached_frames_.end()) {
    if (!frame->second->CanEvict()) {
      throw Exception("frame can't be evicted");
    }
    cached_frames_.erase(frame);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
