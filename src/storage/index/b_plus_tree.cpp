#include <cassert>
#include <cstdlib>
#include <new>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "fmt/core.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
using std::this_thread::get_id;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  LOG_DEBUG("Pool size is %zu", buffer_pool_manager->GetPoolSize());
  LOG_DEBUG("leaf max size %d, internal max size %d", leaf_max_size, internal_max_size);
}

/**
 * @brief
 * Helper function to decide whether current b+tree is empty
 * @return true
 * @return false
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/* Get the node page by page id */
/**
 * @brief
 *
 * @param page_id
 * @return BPlusTreePage*
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPage(page_id_t page_id, RWType rw) -> BPlusTreePage * {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  LOG_DEBUG("Get page is %d, pin count is %d", page_id, page->GetPinCount());
  rw == RWType::READ ? page->RLatch() : rw == RWType::WRITE ? page->WLatch() : void(0);
  auto b_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  return b_tree_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetInternalPage(page_id_t internal_id, RWType rw) -> InternalPage * {
  return reinterpret_cast<InternalPage *>(GetPage(internal_id, rw));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(page_id_t leaf_id, RWType rw) -> LeafPage * {
  return reinterpret_cast<LeafPage *>(GetPage(leaf_id, rw));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPage(OpType op, Transaction *transaction) -> BPlusTreePage * {
  RWType rw = op == OpType::READ ? RWType::READ : RWType::WRITE;
  root_latch_.lock();
  BPlusTreePage *root_page = GetPage(root_page_id_, rw);
  LOG_DEBUG("Is root page %d ? parent page id is %d", root_page->GetPageId(), root_page->GetParentPageId());
  // while(!root_page->IsRootPage()) {
  // UnpinPage(reinterpret_cast<Page *>(root_page), root_page->GetPageId(), false, rw);
  // root_page = GetPage(root_page_id_, rw);
  // }
  assert(root_page->IsRootPage());
  root_page->SetIsCurRoot(true);
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(reinterpret_cast<Page *>(root_page));
  }

  return root_page;
}

/**
 * @brief
 *
 * @param new_page_id
 * @param rw
 * @return BPlusTreePage*
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreatePage(page_id_t *new_page_id, RWType rw) -> BPlusTreePage * {
  Page *page = buffer_pool_manager_->NewPage(new_page_id);
  rw == RWType::READ ? page->RLatch() : page->WLatch();
  auto b_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  return b_tree_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateLeafPage(page_id_t *new_page_id, page_id_t parent_page_id, RWType rw) -> LeafPage * {
  auto new_leaf_page = reinterpret_cast<LeafPage *>(CreatePage(new_page_id, rw));
  new_leaf_page->Init(*new_page_id, parent_page_id, leaf_max_size_);
  return new_leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateInternalPage(page_id_t *new_page_id, page_id_t parent_page_id, RWType rw) -> InternalPage * {
  auto new_internal_page = reinterpret_cast<InternalPage *>(CreatePage(new_page_id, rw));
  new_internal_page->Init(*new_page_id, parent_page_id, internal_max_size_);
  return new_internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinPage(Page *page, page_id_t page_id, bool is_dirty, RWType rw) {
  rw == RWType::UPDATE ? void(0) : rw == RWType::READ ? page->RUnlatch() : page->WUnlatch();
  assert(page->GetPinCount() != 0 && "The page may have written back to disk.");
  // LOG_DEBUG("page %d, pin count is %d", page_id, page->GetPinCount());
  assert(buffer_pool_manager_->UnpinPage(page_id, is_dirty) == true && "Unpin page fail!");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePage(Page *page, page_id_t page_id, bool is_dirty, RWType rw) {
  rw == RWType::READ ? page->RUnlatch() : page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(page_id, is_dirty) == true && "Delete Unpin page fail!");
  LOG_DEBUG("The Delete Pin cout is %d, Page is is %d", page->GetPinCount(), page_id);
  assert(buffer_pool_manager_->DeletePage(page_id) == true && "Delete page fail!");
}

/**
 * @brief
 * Helper function to find the leaf node page
 * @param key
 * @return LeafPage*
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool left_most, OpType op, Transaction *transaction)
    -> LeafPage * {
  auto curr_node_page = GetRootPage(op, transaction);
  if (op == OpType::READ) {
    root_latch_.unlock();
  }
  page_id_t next_page_id;
  while (!curr_node_page->IsLeafPage()) {
    auto internal_node_page = reinterpret_cast<InternalPage *>(curr_node_page);
    next_page_id = left_most ? internal_node_page->ValueAt(0) : internal_node_page->SearchExit(key, comparator_);
    curr_node_page = CrabingPage(next_page_id, curr_node_page->GetPageId(), op, transaction);
  }
  return reinterpret_cast<LeafPage *>(curr_node_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabingPage(page_id_t page_id, page_id_t previous, OpType op, Transaction *transaction)
    -> BPlusTreePage * {
  bool exclusive = op != OpType::READ;
  RWType rw = exclusive ? RWType::WRITE : RWType::READ;
  auto b_tree_page = GetPage(page_id, rw);
  if (!exclusive || b_tree_page->IsSafe(op)) {
    FreePage(previous, rw, transaction);
  }
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(reinterpret_cast<Page *>(b_tree_page));
  }
  return b_tree_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePage(page_id_t cur_id, RWType rw, Transaction *transaction) {
  LOG_DEBUG("Start Free Page");
  bool is_dirty = rw == RWType::WRITE;
  if (transaction == nullptr) {
    assert(rw == RWType::READ && cur_id >= 0);
    auto page = buffer_pool_manager_->FetchPage(cur_id);
    buffer_pool_manager_->UnpinPage(cur_id, true);
    UnpinPage(page, cur_id, true, rw);
    return;
  }
  bool is_cur_root = false;
  for (Page *page : *transaction->GetPageSet()) {
    int page_id = page->GetPageId();
    auto bpt = reinterpret_cast<BPlusTreePage *>(page->GetData());
    is_cur_root = bpt->IsCurRoot();
    if (is_cur_root) {
      bpt->SetIsCurRoot(false);
      root_latch_.unlock();
    }
    if (transaction->GetDeletedPageSet()->find(page_id) != transaction->GetDeletedPageSet()->end()) {
      DeletePage(page, page_id, false, rw);
      transaction->GetDeletedPageSet()->erase(page_id);
    } else {
      LOG_DEBUG("Unpin page %d", page_id);
      UnpinPage(page, page_id, is_dirty || is_cur_root, rw);
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}

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
  LOG_DEBUG("get the value of %ld ", key.ToString());

  if (IsEmpty()) {
    return false;
  }
  auto leaf_node_page = FindLeafPage(key, false, OpType::READ, transaction);
  int index = leaf_node_page->Search(key, comparator_);
  if (index != -1) {
    result->push_back(leaf_node_page->ValueAt(index));
  }
  FreePage(leaf_node_page->GetPageId(), RWType::READ, transaction);
  return index != -1;
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
  size_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10;

  /*== Add a new tree ==*/
  root_latch_.lock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    page_id_t new_root_page_id;
    auto new_root = CreateLeafPage(&new_root_page_id, INVALID_PAGE_ID, RWType::WRITE);
    root_page_id_ = new_root_page_id;
    first_leaf_id_ = last_leaf_id_ = new_root_page_id;
    UpdateRootPageId(0);
    new_root->Insert(key, value, comparator_);
    UnpinPage(reinterpret_cast<Page *>(new_root), new_root->GetPageId(), true, RWType::WRITE);
    root_latch_.unlock();
    LOG_DEBUG("Insert Success");
    return true;
  }
  root_latch_.unlock();

  LOG_DEBUG("\033[1;32;40mThread %zu Insert %ld start find Leaf\033[0m", thread_id, key.ToString());
  auto leaf_node_page = FindLeafPage(key, false, OpType::INSERT, transaction);
  LOG_DEBUG("\033[1;32;40mThread %zu Insert %ld into Page %d Unsafe Set size is %zu\033[0m", thread_id, key.ToString(),
            leaf_node_page->GetPageId(), transaction->GetPageSet()->size());
  // check duplication
  if (!leaf_node_page->Insert(key, value, comparator_)) {
    // Draw(buffer_pool_manager_, "Insert"+std::to_string(key.ToString()) + ".dot");
    LOG_DEBUG("Insert Fail, Thread %zu start Free", thread_id);
    FreePage(leaf_node_page->GetPageId(), RWType::WRITE, transaction);
    return false;
  }

  // check if needing splitting the node
  if (leaf_node_page->NeedSplit()) {
    LOG_DEBUG("Start split");
    SplitLeaf(leaf_node_page, transaction);
  }

  // Draw(buffer_pool_manager_, "Insert"+std::to_string(key.ToString()) + ".dot");
  LOG_DEBUG("Insert Success, Thread %zu start Free", thread_id);
  FreePage(leaf_node_page->GetPageId(), RWType::WRITE, transaction);
  return true;
}

/**
 * @brief Split the leaf page
 *
 * @param over_node
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(LeafPage *over_node, Transaction *transaction) {
  int size = over_node->GetSize();
  int mid_index = over_node->GetMaxSize() / 2;
  KeyType mid_key = over_node->KeyAt(mid_index);
  page_id_t over_node_id = over_node->GetPageId();
  page_id_t new_leaf_id;
  InternalPage *parent_node = nullptr;
  LeafPage *new_leaf = nullptr;
  bool is_root = over_node->IsRootPage();
  // LOG_DEBUG(...);
  /* == get parent node == */
  if (!is_root) {
    new_leaf = CreateLeafPage(&new_leaf_id, over_node->GetParentPageId(), RWType::WRITE);
    parent_node = GetInternalPage(over_node->GetParentPageId(), RWType::UPDATE);
  } else {
    // create new root node page
    page_id_t new_root_page_id;
    auto new_root = CreateInternalPage(&new_root_page_id, INVALID_PAGE_ID, RWType::WRITE);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    new_root->SetFirstPoint(over_node_id);
    over_node->SetParentPageId(root_page_id_);
    new_leaf = CreateLeafPage(&new_leaf_id, new_root_page_id, RWType::WRITE);
    parent_node = new_root;
    transaction->AddIntoPageSet(reinterpret_cast<Page *>(new_root));
  }
  transaction->AddIntoPageSet(reinterpret_cast<Page *>(new_leaf));

  /* == Address the parent level == */
  parent_node->Insert(mid_key, new_leaf_id, comparator_);

  /* == Address the leaf level == */

  // Insert the new leaf in the list
  page_id_t next_page_id = over_node->GetNextPageId();
  new_leaf->SetNextPageId(next_page_id);
  if (over_node->IsLast()) {
    last_leaf_id_ = new_leaf_id;
    LOG_DEBUG("New last leaf is %d", last_leaf_id_);
  }
  over_node->SetNextPageId(new_leaf_id);
  /* = move data = */
  auto left_arr = over_node->GetArray();
  auto right_arr = new_leaf->GetArray();
  /* address the right array */
  std::copy(left_arr + mid_index, left_arr + size, right_arr);
  new_leaf->IncreaseSize(size - mid_index);
  /* address the left array */
  over_node->IncreaseSize(-1 * (size - mid_index));

  /* == check if need to split the parent == */
  if (parent_node->NeedSplit()) {
    SplitInternal(parent_node, transaction);
  }
  if (!is_root) {
    UnpinPage(reinterpret_cast<Page *>(parent_node), parent_node->GetPageId(), true, RWType::UPDATE);
  }
}

/**
 * @brief Split the internal page
 *
 * @param over_node
 * @param new_internal
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(InternalPage *over_node, Transaction *transaction) {
  int size = over_node->GetSize();
  int mid_index = over_node->GetMaxSize() / 2 + 1;
  KeyType mid_key = over_node->KeyAt(mid_index);
  page_id_t new_internal_id;
  InternalPage *new_internal = nullptr;
  page_id_t over_node_id = over_node->GetPageId();
  bool is_root = over_node->IsRootPage();

  /* == get parent node == */
  InternalPage *parent_node = nullptr;

  if (!is_root) {
    new_internal = CreateInternalPage(&new_internal_id, over_node->GetParentPageId(), RWType::WRITE);
    parent_node = GetInternalPage(over_node->GetParentPageId(), RWType::UPDATE);
  } else {
    // create new root node page
    page_id_t new_root_page_id;
    auto new_root = CreateInternalPage(&new_root_page_id, INVALID_PAGE_ID, RWType::WRITE);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    new_root->SetFirstPoint(over_node_id);
    over_node->SetParentPageId(root_page_id_);
    new_internal = CreateInternalPage(&new_internal_id, new_root_page_id, RWType::WRITE);
    parent_node = new_root;
    transaction->AddIntoPageSet(reinterpret_cast<Page *>(parent_node));
  }
  transaction->AddIntoPageSet(reinterpret_cast<Page *>(new_internal));

  /* == Address the parent level == */
  parent_node->Insert(mid_key, new_internal_id, comparator_);

  /* == Address the low level (cut data into the new internal node.) == */
  auto left_arr = over_node->GetArray();
  auto right_arr = new_internal->GetArray();
  page_id_t mid_page_id = left_arr[mid_index].second;

  /* address the right array
    (we have to iterate it, because we want to update the ParentId for the child node.) */
  for (int i = 0; i < size - mid_index; ++i) {
    auto mapp_elem = left_arr[i + mid_index + 1];
    UpdateParentId(mapp_elem.second, new_internal_id);
    right_arr[i + 1] = mapp_elem;
  }
  UpdateParentId(mid_page_id, new_internal_id);
  new_internal->SetFirstPoint(mid_page_id);
  new_internal->IncreaseSize(size - mid_index);
  /* address the left array
    (we don't have to clear the data, we can just decrease the size of the node.) */
  over_node->IncreaseSize(-1 * (size - mid_index + 1));

  if (parent_node->NeedSplit()) {
    SplitInternal(parent_node, transaction);
  }
  if (!is_root) {
    UnpinPage(reinterpret_cast<Page *>(parent_node), parent_node->GetPageId(), true, RWType::UPDATE);
  }
}

/**
 * @brief Update the ParentId, when create the new internal page
 *
 * @param page_id
 * @param p_page_id
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentId(page_id_t page_id, page_id_t p_page_id) {
  auto target_page = GetPage(page_id, RWType::UPDATE);
  target_page->SetParentPageId(p_page_id);
  UnpinPage(reinterpret_cast<Page *>(target_page), page_id, true, RWType::UPDATE);
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
  size_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10;

  // Draw(buffer_pool_manager_, "Last");
  if (IsEmpty()) {
    return;
  }
  LOG_DEBUG("\033[1;31;40mThread %zu Remove %ld start find Leaf\033[0m", thread_id, key.ToString());
  LeafPage *deleting_leaf = FindLeafPage(key, false, OpType::REMOVE, transaction);
  LOG_DEBUG("\033[1;31;40mThread %zu Remove %ld into Page %d, %d key in this page, Unsafe Set size is %zu\033[0m",
            thread_id, key.ToString(), deleting_leaf->GetPageId(), deleting_leaf->GetSize(),
            transaction->GetPageSet()->size());

  if (deleting_leaf->Remove(key, comparator_)) {
    LOG_DEBUG("\033[1;32;40mRemove Success!\033[0m");
    if (deleting_leaf->IsRootPage()) {
      if (deleting_leaf->GetSize() <= 0) {
        transaction->AddIntoDeletedPageSet(deleting_leaf->GetPageId());
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(0);
      }
      LOG_DEBUG("Remove Thread %zu", thread_id);
      FreePage(deleting_leaf->GetPageId(), RWType::WRITE, transaction);
      return;
    }

    if (deleting_leaf->NeedRedsb()) {
      auto parent_internal = GetInternalPage(deleting_leaf->GetParentPageId(), RWType::UPDATE);
      auto debug_page_set = transaction->GetPageSet();

      int target_index = parent_internal->SearchPosition(deleting_leaf->GetPageId());
      int neber_index = target_index >= parent_internal->GetSize() ? target_index - 1 : target_index + 1;
      bool is_last = neber_index < target_index;
      LOG_DEBUG("Try Get Neighbour leaf, parent id is %d, target key is %ld, neber key is %ld",
                parent_internal->GetPageId(), parent_internal->KeyAt(target_index).ToString(),
                parent_internal->KeyAt(neber_index).ToString());
      assert(parent_internal->ValueAt(neber_index) != deleting_leaf->GetPageId());
      LeafPage *neber_leaf = GetLeafPage(parent_internal->ValueAt(neber_index), RWType::WRITE);
      transaction->AddIntoPageSet(reinterpret_cast<Page *>(neber_leaf));
      /* =======================================    Steal     ========================================= */
      if (!StealSibling(deleting_leaf, parent_internal, neber_leaf, target_index, neber_index, is_last)) {
        /* ======================================== Merge ============================================= */
        MergeLeaf(deleting_leaf, parent_internal, neber_leaf, target_index, neber_index, is_last, transaction);
      }
      /* ============================================================================================== */
      // Draw(buffer_pool_manager_, "Last Remve Finish.dot");
      UnpinPage(reinterpret_cast<Page *>(parent_internal), parent_internal->GetPageId(), true, RWType::UPDATE);
    }
  } else {
    LOG_DEBUG("\033[1;31;40mRemove Fail!\033[0m");
  }
  LOG_DEBUG("Remove Thread %zu", thread_id);
  FreePage(deleting_leaf->GetPageId(), RWType::WRITE, transaction);
}

/**
 * @brief Steal (borrow) from the right sibling, when L has only M/2-1 entry.
 *
 * @param deleting_leaf
 * @param value
 * @return true
 * @return false
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StealSibling(LeafPage *deleting_leaf, InternalPage *parent_internal, LeafPage *neber_leaf,
                                  int target_index, int neber_index, bool is_last) -> bool {
  MappingType value;
  if (!(is_last ? neber_leaf->StealLast(&value) : neber_leaf->StealFirst(&value))) {
    return false;
  }
  if (is_last) {
    deleting_leaf->InsertFirst(&value);
    parent_internal->SetKeyAt(target_index, deleting_leaf->KeyAt(0));
  } else {
    deleting_leaf->InsertLast(&value);
    parent_internal->SetKeyAt(neber_index, neber_leaf->KeyAt(0));
  }
  return true;
}

/**
 * @brief Steal (borrow) from the neighbour internal page.
 *
 * @param deleting_internal
 * @return true
 * @return false
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StealInternal(InternalPage *deleting_internal, InternalPage *parent_internal,
                                   InternalPage *neber_internal, int target_index, bool is_last) -> bool {
  // for (int i = 0; i <= deleting_internal->GetSize(); ++i) {
  // LOG_DEBUG("Before Steal The rest internal key has %ld, value is %d", deleting_internal->KeyAt(i).ToString(),
  // deleting_internal->ValueAt(i));
  // }

  std::pair<KeyType, page_id_t> value;
  if (!(is_last ? neber_internal->StealLast(&value) : neber_internal->StealFirst(&value))) {
    return false;
  }
  if (is_last) {
    deleting_internal->InsertFirst(&value);
    // Update the current page
    deleting_internal->SetKeyAt(1, parent_internal->KeyAt(target_index));
    // Update the parent page key
    parent_internal->SetKeyAt(target_index, value.first);
  } else {
    deleting_internal->InsertLast(&value);
    // Update the current page
    deleting_internal->SetKeyAt(deleting_internal->GetSize(), parent_internal->KeyAt(target_index + 1));
    // Update the parent page key
    parent_internal->SetKeyAt(target_index + 1, neber_internal->KeyAt(0));
  }

  // for (int i = 0; i <= deleting_internal->GetSize(); ++i) {
  // LOG_DEBUG("After Steal The rest internal key has %ld, value is %d", deleting_internal->KeyAt(i).ToString(),
  // deleting_internal->ValueAt(i));
  // }

  UpdateParentId(value.second, deleting_internal->GetPageId());
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedsbInternal(InternalPage *deleting_internal, Transaction *transaction) {
  /* = Get the Needed Pages = */
  auto parent_internal = GetInternalPage(deleting_internal->GetParentPageId(), RWType::UPDATE);
  int target_index = parent_internal->SearchPosition(deleting_internal->GetPageId());
  int neber_index = target_index == parent_internal->GetSize() ? target_index - 1 : target_index + 1;
  LOG_DEBUG("Try Get Neighbour Internal!");
  assert(parent_internal->ValueAt(neber_index) != deleting_internal->GetPageId());
  auto neber_internal = GetInternalPage(parent_internal->ValueAt(neber_index), RWType::WRITE);
  bool is_last = neber_index < target_index;
  transaction->AddIntoPageSet(reinterpret_cast<Page *>(neber_internal));
  /* === Steal === */
  if (!StealInternal(deleting_internal, parent_internal, neber_internal, target_index, is_last)) {
    MergeInternal(deleting_internal, parent_internal, neber_internal, target_index, neber_index, is_last, transaction);
  }
  if (parent_internal->IsRootPage() && parent_internal->GetSize() < 1) {
    transaction->AddIntoDeletedPageSet(parent_internal->GetPageId());
    if (is_last) {
      neber_internal->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = neber_internal->GetPageId();
    } else {
      deleting_internal->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = deleting_internal->GetPageId();
    }
    UpdateRootPageId(0);
    // return;
  }
  if (!parent_internal->IsRootPage() && parent_internal->NeedRedsb()) {
    RedsbInternal(parent_internal, transaction);
  }
  UnpinPage(reinterpret_cast<Page *>(parent_internal), parent_internal->GetPageId(), true, RWType::UPDATE);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeInternal(InternalPage *deleting_internal, InternalPage *parent_internal,
                                   InternalPage *neber_internal, int target_index, int neber_index, bool is_last,
                                   Transaction *transaction) {
  LOG_DEBUG("Start Merge Internal, page id is %d", deleting_internal->GetPageId());
  auto deleting_arr = deleting_internal->GetArray();
  auto neber_arr = neber_internal->GetArray();
  if (is_last) {
    transaction->AddIntoDeletedPageSet(deleting_internal->GetPageId());
    deleting_internal->SetKeyAt(0, parent_internal->KeyAt(target_index));
    int neber_size = neber_internal->GetSize();
    for (int i = 0; i <= deleting_internal->GetSize(); ++i) {
      auto mapp_elem = deleting_arr[i];
      UpdateParentId(mapp_elem.second, neber_internal->GetPageId());  // maybe bug
      neber_arr[i + neber_size + 1] = mapp_elem;
    }
    neber_internal->IncreaseSize(deleting_internal->GetSize() + 1);
    parent_internal->Remove(target_index);
  } else {
    transaction->AddIntoDeletedPageSet(neber_internal->GetPageId());
    neber_internal->SetKeyAt(0, parent_internal->KeyAt(neber_index));
    int deleting_size = deleting_internal->GetSize();
    for (int i = 0; i <= neber_internal->GetSize(); ++i) {
      auto mapp_elem = neber_arr[i];
      UpdateParentId(mapp_elem.second, deleting_internal->GetPageId());
      deleting_arr[i + deleting_size + 1] = mapp_elem;
    }
    deleting_internal->IncreaseSize(neber_internal->GetSize() + 1);
    parent_internal->Remove(neber_index);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeaf(LeafPage *rest_leaf, InternalPage *parent_internal, LeafPage *merging_leaf,
                               int target_index, int neber_index, bool is_last, Transaction *transaction) {
  if (is_last) {
    if (rest_leaf->IsLast()) {
      last_leaf_id_ = merging_leaf->GetPageId();
    }
    merging_leaf->MergeFromRight(rest_leaf);
    parent_internal->Remove(target_index);
    LOG_DEBUG("parnet_internal %d remove %d", parent_internal->GetPageId(), target_index);
    // rest_leaf->SetIsDeleted(true);
    transaction->AddIntoDeletedPageSet(rest_leaf->GetPageId());
  } else {
    if (merging_leaf->IsLast()) {
      last_leaf_id_ = rest_leaf->GetPageId();
    }
    rest_leaf->MergeFromRight(merging_leaf);
    parent_internal->Remove(neber_index);
    // merging_leaf->SetIsDeleted(true);
    transaction->AddIntoDeletedPageSet(merging_leaf->GetPageId());
  }
  if (parent_internal->IsRootPage() && parent_internal->GetSize() < 1) {
    is_last ? merging_leaf->SetParentPageId(INVALID_PAGE_ID) : rest_leaf->SetParentPageId(INVALID_PAGE_ID);
    transaction->AddIntoDeletedPageSet(root_page_id_);
    is_last ? root_page_id_ = merging_leaf->GetPageId() : root_page_id_ = rest_leaf->GetPageId();
    UpdateRootPageId(0);
    return;
  }
  if (!parent_internal->IsRootPage() && parent_internal->NeedRedsb()) {
    RedsbInternal(parent_internal, transaction);
  }
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
  LOG_DEBUG("First page is %d", first_leaf_id_);
  return std::move(IndexIterator<KeyType, ValueType, KeyComparator>(GetLeafPage(first_leaf_id_, RWType::UPDATE),
                                                                    buffer_pool_manager_));
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto target_leaf = FindLeafPage(key, false, OpType::READ, nullptr);
  reinterpret_cast<Page *>(target_leaf)->RUnlatch();
  int target_index = target_leaf->Search(key, comparator_);
  assert(target_index != -1);
  return std::move(IndexIterator<KeyType, ValueType, KeyComparator>(target_leaf, buffer_pool_manager_, target_index));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  LOG_DEBUG("last page is %d", last_leaf_id_);
  auto target_leaf = GetLeafPage(last_leaf_id_, RWType::UPDATE);
  return std::move(
      IndexIterator<KeyType, ValueType, KeyComparator>(target_leaf, buffer_pool_manager_, target_leaf->GetSize()));
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() + 1 << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() + 1 << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i <= inner->GetSize(); i++) {
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
    for (int i = 0; i <= inner->GetSize(); i++) {
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
