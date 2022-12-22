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

#include <cassert>
#include <sstream>
#include <utility>
#include <cstring>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

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
  SetPrevPageId(INVALID_PAGE_ID);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrevPageId() const -> page_id_t { return prev_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrevPageId(page_id_t prev_page_id) { prev_page_id_ = prev_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second;
}

/**
 * @brief 
 * 
 * @param key 
 * @param value 
 * @param comparator 
 * @return true 
 * @return false 
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool {
  // check deplicate
  if (Search(key, comparator) != -1) {
    return false;
  }

  IncreaseSize(1);
  int j = GetSize() - 1;
  array_[j] = std::make_pair(key, value);

  int compare = 0;
  while(j > 0 && (compare = comparator(KeyAt(j), KeyAt(j - 1))) == -1) {
    auto temp = array_[j];
    array_[j] = array_[j - 1];
    array_[j - 1] = temp;
    j--;
  }

  return true;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFirst(const MappingType *value) {
  std::copy(array_, array_ + GetSize(), array_ + 1);
  array_[0] = *value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertLast(const MappingType *value) {
  IncreaseSize(1);
  array_[GetSize() - 1] = *value;
}

/**
 * @brief Remove the First item in the array, and return the value.
 * 
 * @param value 
 * @return true 
 * @return false 
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::StealFirst(MappingType * value) -> bool {
  if (GetSize() - 1 < GetMaxSize() / 2) { return false; }
  *value = array_[0];
  std::copy(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
  return true;
}

/**
 * @brief Remove the Last item in the array, and return the value.
 * 
 * @param value 
 * @return true 
 * @return false 
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::StealLast(MappingType * value) -> bool {
  if (GetSize() - 1 < GetMaxSize() / 2) { return false; }
  *value = array_[GetSize() - 1];
  IncreaseSize(-1);
  return true;
}


/* three case to return
 * 1. no key in the array, return false.
 * 2. success remove the value.
 * 3. remove the first value, we need to update the parent key.
 */

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator, bool * need_update) -> bool {
  int index = Search(key, comparator);
  if (index == -1) {
    return false;
  }

  /* check if we need to update the parent key*/
  if (index == 0) {
    *need_update = true;
  }

  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeFromLeft(LeafPage * rest_page) {
  auto size = rest_page->GetSize();
  auto rest_array = rest_page->GetArray(); 

  std::copy(array_, array_ + GetSize(), array_ + size); 
  std::copy(rest_array, rest_array + size, array_);
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeFromRight(LeafPage * rest_page) {
  auto size = rest_page->GetSize();
  auto rest_array = rest_page->GetArray(); 

  std::copy(rest_array, rest_array + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Search(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 0;
  int right = GetSize() - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) == 0) {
      return mid;
    }
    if (comparator(key, KeyAt(mid)) == 1) {
      left = mid + 1;
    } 
    else {
      right = mid - 1;
    }
  }
  
  return -1;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
