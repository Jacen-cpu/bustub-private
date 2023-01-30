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
#include "common/config.h"
#include "common/logger.h"

namespace bustub {
using std::make_pair;

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();

  if (curr_size_ == 0) {
    latch_.unlock();
    return false;
  }

  auto it = no_cached_queue_.begin();
  for (; it != no_cached_queue_.end(); it++) {
    if (frames_.find(*it)->second.is_evi_) {
      break;
    }
  }

  if (it != no_cached_queue_.end()) {
    *frame_id = *it;
    no_cached_queue_.erase(it);
  } else {
    it = cached_queue_.begin();
    for (; it != cached_queue_.end(); it++) {
      if (frames_.find(*it)->second.is_evi_) {
        break;
      }
    }
    *frame_id = *it;
    cached_queue_.erase(it);
  }

  // this is how we find the evict page when all is inf+.
  // get the evi frame
  if (auto search = frames_.find(*frame_id); search != frames_.end()) {
    search->second.is_alive_ = false;
    search->second.ref_time_ = 0;
    search->second.is_cached_ = false;
    curr_size_--;
  }

  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  // check the frame id.
  if (!CheckFrame(frame_id)) {
    latch_.unlock();
    throw "Invalid frame!";
  }

  if (auto search = frames_.find(frame_id); search != frames_.end()) {
    // address died frames
    if (!search->second.is_alive_) {
      search->second.is_alive_ = true;
      if (search->second.is_evi_) {
        curr_size_++;
      }
      no_cached_queue_.push_back(frame_id);
      search->second.ref_time_ = 1;

      latch_.unlock();
      return;
    }

    // address cached frames
    if (search->second.is_cached_) {
      // in the cache
      // update the cached queue
      auto temp_it = std::find(cached_queue_.begin(), cached_queue_.end(), frame_id);
      frame_id_t temp_id = *temp_it;
      cached_queue_.erase(temp_it);
      cached_queue_.push_back(temp_id);

      latch_.unlock();
      return;
    }

    // address no cached frames
    if (search->second.ref_time_ + 1 == k_) {
      search->second.ref_time_ = k_;
      search->second.is_cached_ = true;

      // move the page from no cached queue to cached queue
      auto temp_it = std::find(no_cached_queue_.begin(), no_cached_queue_.end(), frame_id);
      no_cached_queue_.erase(temp_it);
      cached_queue_.push_back(frame_id);
    } else {
      search->second.ref_time_ += 1;
    }
  } else {
    // if not found, create a new entry
    Frame new_attr;
    new_attr.ref_time_ = 1;
    new_attr.is_alive_ = true;
    frames_.insert(make_pair(frame_id, new_attr));
    no_cached_queue_.push_back(frame_id);
  }

  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (!CheckFrame(frame_id)) {
    latch_.unlock();
    throw "Invalid frame!";
  }

  if (auto search = frames_.find(frame_id); search != frames_.end()) {
    if (search->second.is_evi_ ^ set_evictable) {
      if (search->second.is_alive_) {
        curr_size_ += set_evictable ? 1 : -1;
      }
      search->second.is_evi_ = set_evictable;
    }
  }

  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (!CheckFrame(frame_id)) {
    latch_.unlock();
    throw "Invalid frame!";
  }

  if (curr_size_ == 0) {
    latch_.unlock();
    return;
  }

  // remove frame from no cached and cached.
  if (auto search = frames_.find(frame_id); search != frames_.end()) {
    if (search->second.is_alive_) {
      if (!search->second.is_evi_) {
        latch_.unlock();
        throw "non evictable frame!";
      }
      curr_size_--;
      if (search->second.is_cached_) {
        auto temp_it = std::find(cached_queue_.begin(), cached_queue_.end(), frame_id);
        cached_queue_.erase(temp_it);
        search->second.is_alive_ = false;
        search->second.ref_time_ = 0;
        search->second.is_cached_ = false;
      } else {
        auto temp_it = std::find(no_cached_queue_.begin(), no_cached_queue_.end(), frame_id);
        no_cached_queue_.erase(temp_it);
        search->second.is_alive_ = false;
        search->second.ref_time_ = 0;
      }
    }
  }

  latch_.unlock();
}

// it's ok
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}
}  // namespace bustub
