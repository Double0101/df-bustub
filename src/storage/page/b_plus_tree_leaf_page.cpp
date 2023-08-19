//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);

  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyValueAt(int index) const -> MappingType { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetArray() -> MappingType * { return array_; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Exist(const KeyType &key, const KeyComparator &comparator) -> bool {
  int idx = 0;
  while (idx < GetSize() && comparator(key, KeyAt(idx)) < 0) {
    ++idx;
  }
  return idx != GetSize() && comparator(key, KeyAt(idx)) == 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int idx;
  for (idx = GetSize() - 1; idx >= 0 && comparator(key, KeyAt(idx)) < 0; --idx) {
    array_[idx + 1] = array_[idx];
  }
  array_[idx + 1] = {key, value};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAndSplit(const KeyType &key, const ValueType &value,
                                                B_PLUS_TREE_LEAF_PAGE_TYPE *new_page,
                                                const KeyComparator &comparator) -> void {
  int pivot = (GetSize() - 1) > 1;
  KeyType pivot_key = array_[pivot].first;
  MappingType *new_array = new_page->GetArray();

  int i;
  int j = 0;
  if (comparator(key, pivot_key) > 0) {
    for (i = pivot + 1; i < GetSize(); ++i) {
      new_array[j++] = array_[i];
    }
    new_page->SetSize(j);
    new_page->Insert(key, value, comparator);
    SetSize(pivot + 1);
  } else {
    for (i = pivot; i < GetSize(); ++i) {
      new_array[j++] = array_[i];
    }
    new_page->SetSize(j);
    SetSize(pivot);
    Insert(key, value, comparator);
  }

  new_page->SetNextPageId(next_page_id_);
  next_page_id_ = new_page->GetPageId();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
