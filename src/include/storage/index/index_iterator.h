//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "concurrency/transaction.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index, Page *page,
                         std::string index_name, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return cur_idx_ == itr.cur_idx_ && leaf_page_ == itr.leaf_page_ && index_name_ == itr.index_name_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return cur_idx_ != itr.cur_idx_ || leaf_page_ != itr.leaf_page_;
  }

 private:
  // add your own private member variables here
  std::string index_name_;
  Page *cur_page_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  BufferPoolManager *buffer_pool_manager_;
  int cur_idx_{-1};
  MappingType *array_;
};

}  // namespace bustub
