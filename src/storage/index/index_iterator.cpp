/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, BufferPoolManager *buffer_pool_manager)
    : leaf_page_(leaf_page), array_(leaf_page->GetArray()) {
  buffer_pool_manager_ = buffer_pool_manager;
  cur_idx_ = 0;
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page_ != nullptr && leaf_page_->GetPageId() != INVALID_PAGE_ID) {
    // TODO: is_dirty may not be false
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
  }
};  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return cur_idx_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return array_[cur_idx_]; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ++cur_idx_;
  if (cur_idx_ == leaf_page_->GetSize()) {
    if (leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
      page_id_t next_page = leaf_page_->GetNextPageId();
      // TODO: is_dirty may not be false
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
      Page *page = buffer_pool_manager_->FetchPage(next_page);
      leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
      array_ = leaf_page_->GetArray();
      cur_idx_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
