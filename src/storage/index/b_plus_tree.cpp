#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  Page *page = FindLeafPage(key, READ_MODE, transaction);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int idx = 0;
  while (idx < leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(idx)) > 0) {
    ++idx;
  }
  bool res = comparator_(key, leaf_page->KeyAt(idx)) == 0;
  if (res) {
    result->push_back(leaf_page->ValueAt(idx));
  }
  page->RUnlatch();
  ClearTransPages(READ_MODE, transaction);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  page_id_t page_id;
  Page *page;
  BPlusTreePage *b_page;
  LeafPage *leaf_page;
  bool res;

  root_latch_.lock();
  if (IsEmpty()) {
    // Create new root page and insert
    page = buffer_pool_manager_->NewPage(&page_id);
    b_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    root_page_id_ = page_id;
    leaf_page = reinterpret_cast<LeafPage *>(b_page);
    leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
    res = leaf_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(page_id, true);
    UpdateRootPageId(1);
    root_latch_.unlock();
    return res;
  }
  root_latch_.unlock();

  transaction->AddIntoPageSet(BEFORE_ROOT_PAGE);
  page = FindLeafPage(key, INSERT_MODE, transaction);
  leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  if (leaf_page->Exist(key, comparator_)) {
    return false;
  }
  if (!leaf_page->IsFull()) {
    ClearTransPages(INSERT_MODE, transaction);
    res = leaf_page->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return res;
  } else {
    res = InsertUpforward(key, value, page, transaction);
    ClearTransPages(INSERT_MODE, transaction);
    return res;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertUpforward(const KeyType &key, const ValueType &value, Page *page, Transaction *transaction)
    -> bool {
  // insert into leaf page
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->InsertAndSplit(key, value, new_leaf, comparator_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  // the pair insert into up page
  std::pair<KeyType, page_id_t> new_map = std::make_pair(new_leaf->KeyAt(0), new_page_id);
  buffer_pool_manager_->UnpinPage(new_page_id, true);

  // insert into internal page upforward
  auto page_set = transaction->GetPageSet();
  Page *upper_page = page_set->back();
  Page *lower_page;
  page_set->pop_back();
  BPlusTreePage *lower_bp;
  InternalPage *upper_bp;
  InternalPage *upper_new_bp;
  while (upper_page != BEFORE_ROOT_PAGE) {
    upper_bp = reinterpret_cast<InternalPage *>(upper_page->GetData());
    if (!upper_bp->IsFull()) {
      upper_bp->Insert(new_map.first, new_map.second, comparator_);
      // update low page parent page id
      lower_page = buffer_pool_manager_->FetchPage(new_map.second);
      lower_page->WLatch();
      lower_bp = reinterpret_cast<BPlusTreePage *>(lower_page->GetData());
      lower_bp->SetParentPageId(upper_page->GetPageId());
      lower_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(lower_page->GetPageId(), true);

      upper_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(upper_page->GetPageId(), true);
      return true;
    }
    new_page = buffer_pool_manager_->NewPage(&new_page_id);
    upper_new_bp = reinterpret_cast<InternalPage *>(new_page->GetData());
    upper_new_bp->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_map.first = upper_bp->InsertAndSplit(new_map.first, new_map.second, upper_new_bp, comparator_);
    new_map.second = new_page_id;
    // update parent page id in lower page
    for (int i = 0; i < upper_new_bp->GetSize(); ++i) {
      lower_page = buffer_pool_manager_->FetchPage(upper_new_bp->ValueAt(i));
      lower_page->WLatch();
      lower_bp = reinterpret_cast<BPlusTreePage *>(lower_page->GetData());
      lower_bp->SetParentPageId(upper_new_bp->GetPageId());
      lower_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(lower_page->GetPageId(), true);
    }
    upper_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(upper_page->GetPageId(), true);
    upper_page = page_set->back();
    page_set->pop_back();
  }

  // when upforward insert step by step come into root page and root page full
  // create new root page replace old one
  root_latch_.lock();
  new_page = buffer_pool_manager_->NewPage(&new_page_id);
  upper_bp = reinterpret_cast<InternalPage *>(new_page->GetData());
  upper_bp->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  upper_bp->SetSize(2);
  upper_bp->SetValueAt(0, root_page_id_);
  upper_bp->SetKeyAt(1, new_map.first);
  upper_bp->SetValueAt(1, new_map.second);
  for (int i = 0; i < upper_bp->GetSize(); ++i) {
    lower_page = buffer_pool_manager_->FetchPage(upper_bp->ValueAt(i));
    lower_page->WLatch();
    lower_bp = reinterpret_cast<BPlusTreePage *>(lower_page->GetData());
    lower_bp->SetParentPageId(upper_bp->GetPageId());
    lower_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(lower_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);
  root_latch_.unlock();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  Page *lower_page;
  LeafPage *leaf_page;
  if (IsEmpty()) {
    return;
  }
  transaction->AddIntoPageSet(BEFORE_ROOT_PAGE);
  lower_page = FindLeafPage(key, DELETE_MODE, transaction);
  leaf_page = reinterpret_cast<LeafPage *>(lower_page->GetData());
  leaf_page->Delete(key, comparator_);
  if (!leaf_page->IsRootPage() && leaf_page->GetSize() < leaf_page->GetMinSize()) {
    auto page_set = transaction->GetPageSet();
    Page *upper_page = page_set->back();
    page_set->pop_back();
    auto *upper_ip = reinterpret_cast<InternalPage *>(upper_page->GetData());
    int idx = 0;
    auto *array = upper_ip->GetArray();
    while (array[idx].second != lower_page->GetPageId()) {
      ++idx;
    }
    if (!LeafBorrow(idx, leaf_page, upper_ip)) {
      LeafMerge(idx, leaf_page, upper_ip);
      InternalPage *lower_ip;
      while (!upper_ip->IsRootPage() && upper_ip->GetSize() < upper_ip->GetMinSize()) {
        lower_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(lower_page->GetPageId(), true);
        lower_page = upper_page;
        upper_page = page_set->back();
        page_set->pop_back();
        lower_ip = reinterpret_cast<InternalPage *>(lower_page->GetData());
        upper_ip = reinterpret_cast<InternalPage *>(upper_page->GetData());
        idx = 0;
        array = upper_ip->GetArray();
        while (array[idx].second != lower_page->GetPageId()) {
          ++idx;
        }
        if (!InternalBorrow(idx, lower_ip, upper_ip)) {
          InternalMerge(idx, lower_ip, upper_ip);
        }
      }
    }
  }
  lower_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(lower_page->GetPageId(), true);
  ClearTransPages(DELETE_MODE, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalBorrow(int idx, InternalPage *lower_page, InternalPage *upper_page) -> bool {
  Page *brw_page;
  InternalPage *brw_inte_page;
  bool res = false;
  if (idx > 0) {
    brw_page = buffer_pool_manager_->FetchPage(upper_page->ValueAt(idx - 1));
    brw_page->WLatch();
    brw_inte_page = reinterpret_cast<InternalPage *>(brw_page->GetData());
    if (brw_inte_page->GetSize() > brw_inte_page->GetMinSize()) {
      auto brw_one = brw_inte_page->KeyValueAt(brw_inte_page->GetSize() - 1);
      brw_inte_page->SetSize(brw_inte_page->GetSize() - 1);
      lower_page->Insert(brw_one.first, brw_one.second, comparator_);
      upper_page->SetKeyAt(idx, lower_page->KeyAt(0));
      res = true;
    }
    brw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(brw_page->GetPageId(), res);
  }
  if (!res && idx + 1 < upper_page->GetSize()) {
    brw_page = buffer_pool_manager_->FetchPage(upper_page->ValueAt(idx + 1));
    brw_page->WLatch();
    brw_inte_page = reinterpret_cast<InternalPage *>(brw_page->GetData());
    if (brw_inte_page->GetSize() > brw_inte_page->GetMinSize()) {
      auto brw_one = brw_inte_page->KeyValueAt(0);
      brw_inte_page->Delete(0);
      lower_page->Insert(brw_one.first, brw_one.second, comparator_);
      upper_page->SetKeyAt(idx + 1, brw_inte_page->KeyAt(0));
      res = true;
    }
    brw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(brw_page->GetPageId(), res);
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalMerge(int idx, InternalPage *lower_page, InternalPage *upper_page) -> bool {
  Page *mge_page;
  InternalPage *mge_inte_page;
  bool res = false;

  if (idx > 0) {
    mge_page = buffer_pool_manager_->FetchPage(upper_page->ValueAt(idx - 1));
    mge_page->WLatch();
    mge_inte_page = reinterpret_cast<InternalPage *>(mge_page->GetData());
    if (mge_inte_page->GetSize() + lower_page->GetSize() <= lower_page->GetMaxSize()) {
      auto *array = lower_page->GetArray();
      for (int i = 0; i < lower_page->GetSize(); ++i) {
        array[i + mge_inte_page->GetSize()] = array[i];
        array[i] = mge_inte_page->KeyValueAt(i);
      }
      lower_page->SetSize(mge_inte_page->GetSize() + lower_page->GetSize());
      upper_page->SetKeyAt(idx, array[0].first);
      upper_page->Delete(idx - 1);
      res = true;
    }
    mge_page->WUnlatch();
    if (res) {
      buffer_pool_manager_->DeletePage(mge_page->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(mge_page->GetPageId(), false);
    }
  }

  if (!res && idx + 1 < upper_page->GetSize()) {
    mge_page = buffer_pool_manager_->FetchPage(upper_page->ValueAt(idx + 1));
    mge_page->WLatch();
    mge_inte_page = reinterpret_cast<InternalPage *>(mge_page->GetData());
    if (mge_inte_page->GetSize() + lower_page->GetSize() <= mge_inte_page->GetMaxSize()) {
      auto *array = lower_page->GetArray();
      for (int i = 0; i < mge_inte_page->GetSize(); ++i) {
        array[i + lower_page->GetSize()] = mge_inte_page->KeyValueAt(i);
      }
      lower_page->SetSize(mge_inte_page->GetSize() + lower_page->GetSize());
      upper_page->Delete(idx + 1);
      res = true;
    }
    mge_page->WUnlatch();
    if (res) {
      buffer_pool_manager_->DeletePage(mge_page->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(mge_page->GetPageId(), false);
    }
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafMerge(int idx, LeafPage *leaf_page, InternalPage *upper_page) -> bool {
  Page *mge_page;
  LeafPage *mge_leaf_page;
  bool res = false;

  if (idx > 0) {
    mge_page = buffer_pool_manager_->FetchPage(upper_page->ValueAt(idx - 1));
    mge_page->WLatch();
    mge_leaf_page = reinterpret_cast<LeafPage *>(mge_page->GetData());
    if (mge_leaf_page->GetSize() + leaf_page->GetSize() <= mge_leaf_page->GetMaxSize()) {
      auto *array = leaf_page->GetArray();
      for (int i = 0; i < leaf_page->GetSize(); ++i) {
        array[i + mge_leaf_page->GetSize()] = array[i];
        array[i] = mge_leaf_page->KeyValueAt(i);
      }
      leaf_page->SetSize(mge_leaf_page->GetSize() + leaf_page->GetSize());
      upper_page->SetKeyAt(idx, array[0].first);
      upper_page->Delete(idx - 1);
      res = true;
    }
    mge_page->WUnlatch();
    if (res) {
      buffer_pool_manager_->DeletePage(mge_page->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(mge_page->GetPageId(), false);
    }
  }

  if (!res && idx + 1 < upper_page->GetSize()) {
    mge_page = buffer_pool_manager_->FetchPage(upper_page->ValueAt(idx + 1));
    mge_page->WLatch();
    mge_leaf_page = reinterpret_cast<LeafPage *>(mge_page->GetData());
    if (mge_leaf_page->GetSize() + leaf_page->GetSize() <= mge_leaf_page->GetMaxSize()) {
      auto *array = leaf_page->GetArray();
      for (int i = 0; i < mge_leaf_page->GetSize(); ++i) {
        array[i + leaf_page->GetSize()] = mge_leaf_page->KeyValueAt(i);
      }
      leaf_page->SetSize(mge_leaf_page->GetSize() + leaf_page->GetSize());
      upper_page->Delete(idx + 1);
      res = true;
    }
    mge_page->WUnlatch();
    if (res) {
      buffer_pool_manager_->DeletePage(mge_page->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(mge_page->GetPageId(), false);
    }
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafBorrow(int idx, LeafPage *leaf_page, InternalPage *upper_page) -> bool {
  Page *brw_page;
  LeafPage *brw_leaf_page;
  bool res = false;

  // borrow from left
  if (idx - 1 >= 0) {
    brw_page = buffer_pool_manager_->FetchPage(upper_page->ValueAt(idx - 1));
    brw_page->WLatch();
    brw_leaf_page = reinterpret_cast<LeafPage *>(brw_page->GetData());
    if (brw_leaf_page->GetSize() > brw_leaf_page->GetMinSize()) {
      auto brw_one = brw_leaf_page->KeyValueAt(brw_leaf_page->GetSize() - 1);
      brw_leaf_page->SetSize(brw_leaf_page->GetSize() - 1);
      leaf_page->Insert(brw_one.first, brw_one.second, comparator_);
      upper_page->SetKeyAt(idx, leaf_page->KeyAt(0));
      res = true;
    }
    brw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(brw_page->GetPageId(), res);
  }
  // borrow from right
  if (!res && idx + 1 < upper_page->GetSize()) {
    brw_page = buffer_pool_manager_->FetchPage(upper_page->ValueAt(idx + 1));
    brw_page->WLatch();
    brw_leaf_page = reinterpret_cast<LeafPage *>(brw_page->GetData());
    if (brw_leaf_page->GetSize() > brw_leaf_page->GetMinSize()) {
      auto brw_one = brw_leaf_page->KeyValueAt(0);
      brw_leaf_page->Delete(0);
      leaf_page->Insert(brw_one.first, brw_one.second, comparator_);
      upper_page->SetKeyAt(idx + 1, brw_leaf_page->KeyAt(0));
      res = true;
    }
    brw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(brw_page->GetPageId(), res);
  }
  return res;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  page_id_t page_id = root_page_id_;
  page_id_t pre_page_id;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  page->RLatch();
  InternalPage *bpip;
  auto *bpp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!bpp->IsLeafPage()) {
    bpip = reinterpret_cast<InternalPage *>(bpp);
    pre_page_id = page_id;
    page_id = bpip->ValueAt(0);

    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(pre_page_id, false);
    page = buffer_pool_manager_->FetchPage(page_id);
    page->RLatch();
    bpp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  auto *leaf_page = reinterpret_cast<LeafPage *>(bpp);
  return INDEXITERATOR_TYPE(leaf_page, 0, page, index_name_, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  Page *page = FindLeafPage(key, READ_MODE, nullptr);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int idx = 0;
  while (idx < leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(idx)) > 0) {
    ++idx;
  }
  return idx < leaf_page->GetSize() ? INDEXITERATOR_TYPE(leaf_page, idx, page, index_name_, buffer_pool_manager_)
                                    : End();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, int mode, bustub::Transaction *transaction) -> Page * {
  page_id_t page_id = root_page_id_;
  page_id_t pre_page_id;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  mode == READ_MODE ? page->RLatch() : page->WLatch();
  InternalPage *bpip;
  auto *bpp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!bpp->IsLeafPage()) {
    bpip = reinterpret_cast<InternalPage *>(bpp);
    pre_page_id = page_id;
    int i = 1;
    if (bpip->GetSize() > 1) {
      while (i < bpip->GetSize() && comparator_(key, bpip->KeyAt(i)) >= 0) {
        ++i;
      }
    }
    page_id = bpip->ValueAt(i - 1);

    switch (mode) {
      case INSERT_MODE: {
        if (!bpip->IsFull()) {
          // safe to insert
          ReleaseBeforePages(INSERT_MODE, transaction);
        }
        transaction->AddIntoPageSet(page);
      } break;
      case DELETE_MODE: {
        if (bpip->GetSize() > (bpip->GetMaxSize() + 1) / 2) {
          // safe to delete
          ReleaseBeforePages(DELETE_MODE, transaction);
        }
        transaction->AddIntoPageSet(page);
      } break;
      default: {
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(pre_page_id, false);
      } break;
    }
    page = buffer_pool_manager_->FetchPage(page_id);
    mode == READ_MODE ? page->RLatch() : page->WLatch();
    bpp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReleaseBeforePages(int mode, Transaction *transaction) -> void {
  auto page_set = transaction->GetPageSet();
  if (page_set->empty()) {
    return;
  }
  Page *page = page_set->back();
  while (page != BEFORE_ROOT_PAGE) {
    page_set->pop_back();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ClearTransPages(int mode, Transaction *transaction) -> void {
  if (transaction == nullptr) {
    return;
  }
  auto page_set = transaction->GetPageSet();
  if (page_set->empty()) {
    return;
  }
  Page *page = page_set->back();
  while (page != BEFORE_ROOT_PAGE) {
    page_set->pop_back();
    mode == READ_MODE ? page->RUnlatch() : page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  page_set->pop_back();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(nullptr, -1, nullptr, index_name_, nullptr);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
