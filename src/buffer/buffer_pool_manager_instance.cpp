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

#include "common/exception.h"
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

auto BufferPoolManagerInstance::GetEmptyPage(frame_id_t *frame_id) -> bool {
  Page *page;
  free_list_latch_.lock();
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    free_list_latch_.unlock();
    return true;
  }
  free_list_latch_.unlock();
  if (!replacer_->Evict(frame_id)) { return false; }
  page = pages_ + *frame_id;
  page_table_->Remove(page->page_id_);
  return true;
}

auto BufferPoolManagerInstance::ResetPage(Page *page) -> void {
  if (page->IsDirty()) {
    FlushPgImp(page->page_id_);
  }
  page->ResetMemory();
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  bool res = false;
  Page *page = nullptr;
  res = GetEmptyPage(&frame_id);
  if (!res) { return nullptr; }
  page = pages_ + frame_id;
  ResetPage(page);
  page_id_t pid = AllocatePage();
  *page_id = pid;
  page_table_->Insert(pid, frame_id);
  replacer_->RecordAccess(frame_id);
  page->pin_count_ = 1;
  page->page_id_ = pid;
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  alloc_latch_.WLock();
  frame_id_t frame_id = 0;
  bool res = page_table_->Find(page_id, frame_id);
  Page *page = pages_ + frame_id;
  if (!res) {
    res = GetEmptyPage(&frame_id);
    if (!res) { return nullptr; }
    page = pages_ + frame_id;
    disk_manager_->ReadPage(page_id, page->data_);
    page->page_id_ = page_id;
    page_table_->Insert(page_id, frame_id);
  }
  page->WLatch();
  page->pin_count_ += 1;
  page->WUnlatch();
  replacer_->RecordAccess(frame_id);
  alloc_latch_.WUnlock();
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  bool res;
  Page *page;
  res = page_table_->Find(page_id, frame_id);
  if (!res) { return false; }

  page = pages_ + frame_id;
  if (is_dirty) {
    FlushPgImp(page_id);
  }
  page->WLatch();
  if (page->GetPinCount() == 0) {
    page->WUnlatch();
    return false;
  }
  --(page->pin_count_);
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  page->WUnlatch();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  bool res;
  Page *page;
  res = page_table_->Find(page_id, frame_id);
  if (!res) { return false; }
  page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  Page *page;
  for (size_t i = 0; i < pool_size_; ++i) {
    page = pages_ + i;
    if (page->GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  bool res;
  Page *page;
  res = page_table_->Find(page_id, frame_id);
  if (!res) { return true; }
  page = pages_ + frame_id;
  page->RLatch();
  if (page->pin_count_ > 0) {
    page->RUnlatch();
    return false;
  }

  DeallocatePage(page_id);
  page->RUnlatch();
  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::DeallocatePage(page_id_t page_id) -> void {
  frame_id_t frame_id;
  bool res;
  Page *page;
  res = page_table_->Find(page_id, frame_id);
  if (res) {
    page = pages_ + frame_id;
    ResetPage(page);
    free_list_latch_.lock();
    free_list_.push_back(frame_id);
    free_list_latch_.unlock();
    replacer_->Remove(frame_id);
    page_table_->Remove(page_id);
  }
}
}  // namespace bustub
