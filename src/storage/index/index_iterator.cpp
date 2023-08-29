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
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index, Page *page,
                                  std::string index_name, BufferPoolManager *buffer_pool_manager)
    : leaf_page_(leaf_page), array_(leaf_page->GetArray()) {
  buffer_pool_manager_ = buffer_pool_manager;
  cur_idx_ = index;
  cur_page_ = page;
  index_name_ = index_name;
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (cur_page_ != nullptr) {
    cur_page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
  }
};  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return (cur_idx_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() == INVALID_PAGE_ID) || cur_idx_ == -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return array_[cur_idx_]; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (cur_idx_ == -1) { return *this; }
  ++cur_idx_;
  if (cur_idx_ == leaf_page_->GetSize()) {
    page_id_t next_page = leaf_page_->GetNextPageId();
    cur_page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
    cur_page_ = nullptr;
    leaf_page_ = nullptr;
    array_ = nullptr;
    cur_idx_ = -1;
    if (next_page != INVALID_PAGE_ID) {
      cur_page_ = buffer_pool_manager_->FetchPage(next_page);
      cur_page_->RLatch();
      leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(cur_page_->GetData());
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
