//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <memory>
#include <utility>
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(0) {
  dir_.insert(dir_.begin(), GetNewBucket());
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  if (dir_.size() == 1) {
    return 1;
  }
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  latch_.lock();

  int dir_index = IndexOf(key);
  auto target_bucket = dir_.at(dir_index);

  if (!target_bucket) {
    latch_.unlock();
    return false;
  }

  bool res = target_bucket->Find(key, value);
  latch_.unlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_.lock();
  int dir_index = IndexOf(key);

  auto target_it = dir_.begin() + dir_index;
  auto target_bucket = *target_it;

  if (!target_bucket) {
    latch_.unlock();
    return false;
  }

  bool res = target_bucket->Remove(key);

  // remove the bucket if it's empty.
  if (target_bucket->IsEmpty()) {
    num_buckets_--;
  }

  latch_.unlock();
  return res;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  int dir_index = IndexOf(key);

  auto target_bucket = dir_.at(dir_index);

  if (!target_bucket) {
    *(dir_.begin() + dir_index) = GetNewBucket();
    target_bucket = dir_.at(dir_index);
  }

  if (target_bucket->IsEmpty()) {
    num_buckets_++;
  }

  // It's very tedious!
  // we may change the target_bucket after redistribution.
  while (!target_bucket->Insert(key, value)) {
    if (target_bucket->GetDepth() == global_depth_) {
      // increase the global depth.
      global_depth_++;

      // double the dir_ size.
      int old_dir_size = dir_.size();
      dir_.resize(2 * old_dir_size);
      std::copy(dir_.begin(), dir_.begin() + old_dir_size, dir_.begin() + old_dir_size);
    }
    // above code is OK.

    RedistributeBucket(target_bucket);
    num_buckets_++;

    // update the target_bucket
    target_bucket = dir_.at(IndexOf(key));
  }
  latch_.unlock();
}

// the default distribution method
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  bucket->IncrementDepth();
  int new_depth = bucket->GetDepth();

  // auto &bro_items = bro_bucket->GetItems();
  // create a new bucket.
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, new_depth);

  auto &new_items = new_bucket->GetItems();
  auto &items = bucket->GetItems();

  // ---rehash and split the bucket---
  auto it = items.begin();
  size_t first_index = ReHash(it->first, new_depth);
  while (it != items.end()) {
    size_t rehash = ReHash(it->first, new_depth);
    if (rehash == first_index) {
      new_items.push_back(*it);
      it = items.erase(it);
    } else {
      ++it;
    }
  }

  // we get the two bucket, next we need to update the dir_.
  // size_t power = global_depth_ - new_depth;
  size_t head_num = pow(2, global_depth_ - new_depth);
  size_t dir_index = 0;
  for (size_t i = 0; i < head_num; ++i) {
    dir_index = (i << new_depth) + first_index;
    *(dir_.begin() + dir_index) = new_bucket;
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ReHash(const K &key, int depth) -> size_t {
  int mask = (1 << depth) - 1;
  return std::hash<K>()(key) & mask;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  int index = 0;
  if (FindIndex(key, index)) {
    auto it = list_.begin();
    std::advance(it, index);
    value = (*it).second;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  int index = 0;
  if (!FindIndex(key, index)) {
    return false;
  }
  auto it = list_.begin();
  std::advance(it, index);

  list_.erase(it);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  int index = 0;

  if (IsFull()) {
    return false;
  }

  if (FindIndex(key, index)) {
    auto it = list_.begin();
    std::advance(it, index);

    (*it).second = value;
    return true;
  }

  list_.push_back(move(std::make_pair(key, value)));
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::FindIndex(const K &key, int &index) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      index = std::distance(list_.begin(), it);
      return true;
    }
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
