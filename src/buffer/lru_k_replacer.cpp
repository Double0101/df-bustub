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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k), replacer_size_(num_frames) {
  evictable_.resize(replacer_size_, false);
  counter_.resize(replacer_size_, 0);
  cache_queue_[0] = std::make_shared<std::list<Query>>();
  cache_queue_[1] = std::make_shared<std::list<Query>>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  for (size_t level = 0; level < 2; ++level) {
    std::shared_ptr<std::list<Query>> ql = cache_queue_[level];
    auto it = ql->begin();
    for (; it != ql->end(); ++it) {
      if (evictable_[it->frame_id_]) {
        *frame_id = it->frame_id_;
        while (it != ql->end()) {
          if (it->frame_id_ == *frame_id) {
            it = ql->erase(it);
          } else {
            ++it;
          }
        }
        // clear associated info
        counter_[*frame_id] = 0;
        SetEvictable(*frame_id, false);
        return true;
      }
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < (int32_t)replacer_size_, "Unexpect frame id");
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  ++counter_[frame_id];
  if (counter_[frame_id] == k_) {
    CacheUpgrade(frame_id);
  }
  if (counter_[frame_id] >= k_) {
    cache_queue_[1]->emplace_back(frame_id, curr_time_++);
    if (counter_[frame_id] == k_) {
      return;
    }
    counter_[frame_id] = k_;
    auto it = cache_queue_[1]->begin();
    while (it != cache_queue_[1]->end()) {
      if (it->frame_id_ == frame_id) {
        cache_queue_[1]->erase(it);
        return;
      }
      ++it;
    }
  } else {
    cache_queue_[0]->emplace_back(frame_id, curr_time_++);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < (int32_t)replacer_size_, "Unexpect frame id");
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  if (set_evictable != evictable_[frame_id]) {
    curr_size_ += (set_evictable - evictable_[frame_id]);
    evictable_[frame_id] = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < (int32_t)replacer_size_, "Unexpect frame id");
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  std::shared_ptr<std::list<Query>> query_list = cache_queue_[0];
  if (counter_[frame_id] >= k_) {
    query_list = cache_queue_[1];
  }
  auto it = query_list->begin();
  while (it != query_list->end()) {
    if (it->frame_id_ == frame_id) {
      it = query_list->erase(it);
    } else {
      ++it;
    }
  }
  counter_[frame_id] = 0;
  SetEvictable(frame_id, false);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::CacheUpgrade(frame_id_t frame_id) -> void {
  std::shared_ptr<std::list<Query>> ql1 = cache_queue_[0];
  std::shared_ptr<std::list<Query>> ql2 = cache_queue_[1];
  auto it = ql1->begin();
  while (it != ql1->end()) {
    if (it->frame_id_ == frame_id) {
      ql2->emplace_back(it->frame_id_, it->query_time_);
      it = ql1->erase(it);
    } else {
      ++it;
    }
  }
  ql2->sort();
}

}  // namespace bustub
