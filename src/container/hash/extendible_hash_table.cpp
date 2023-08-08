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

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
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
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[dir_idx];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[dir_idx];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[dir_idx];
  while (!bucket->Insert(key, value)) {
    RedistributeBucket(dir_idx, bucket);
    dir_idx = IndexOf(key);
    bucket = dir_[dir_idx];
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(size_t idx, std::shared_ptr<Bucket> bucket) -> void {
  size_t n_idx = idx | (1 << bucket->GetDepth());
  bucket->IncrementDepth();

  // resize dir_
  if (bucket->GetDepth() > global_depth_) {
    int old_mask = (1 << global_depth_) - 1;
    global_depth_ += 1;
    num_buckets_ <<= 1;
    dir_.resize(num_buckets_);
    for (int n = num_buckets_ >> 1; n < num_buckets_; ++n) {
      dir_[n] = dir_[n & old_mask];
    }
  }
  // add new Bucket
  dir_[n_idx]= std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  // put some elements into new bucket
  int new_mask = (1 << bucket->GetDepth()) - 1;
  std::shared_ptr<Bucket> nb = dir_[n_idx];
  std::list<std::pair<K, V>> &blst = bucket->GetItems();
  auto it = blst.begin();
  while (it != blst.end()) {
    K key = it->first;
    if ((std::hash<K>()(key) & new_mask) == n_idx) {
      nb->Insert(key, it->second);
      it = blst.erase(it);
    } else {
      ++it;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    std::pair p = *it;
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    std::pair p = *it;
    if (p.first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto it = list_.begin();
  for (; it != list_.end(); ++it) {
    std::pair p = *it;
    if (p.first == key) {
      p.second = value;
      return true;
    }
  }
  if (IsFull()) return false;
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
