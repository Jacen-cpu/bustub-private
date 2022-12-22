//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool {
  // check deplicate
  if (Search(key, comparator) != 0) {
    return false;
  }

  IncreaseSize(1);

  int cur_size = GetSize();
  array_[cur_size] = std::make_pair(key, value);

  int compare = 0;
  int j = cur_size;
  while(j > 1 && (compare = comparator(KeyAt(j), KeyAt(j - 1))) == -1) {
    auto temp = array_[j];
    array_[j] = array_[j - 1];
    array_[j - 1] = temp;
    j--;
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const MappingType *value) {
  std::copy(array_, array_ + GetSize() + 1, array_ + 1);
  array_[0] = *value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertLast(const MappingType *value) {
  IncreaseSize(1);
  array_[GetSize()] = *value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  std::copy(array_ + index + 1, array_ + GetSize() + 1, array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::NeedRedsb() -> bool {
  return IsRootPage() ? GetSize() < 1 : GetSize() < (GetMaxSize() + 1) / 2 - 1;
}

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeFromLeft(InternalPage * rest_page) {
  // auto size = rest_page->GetSize();
  // auto rest_array = rest_page->GetArray(); 

  // std::copy(array_, array_ + GetSize() + 1, array_ + size + 1); 
  // std::copy(rest_array, rest_array + size, array_);
  // IncreaseSize(size);
// }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeFromRight(InternalPage * rest_page) {
  // auto size = rest_page->GetSize();
  // auto rest_array = rest_page->GetArray(); 

  // std::copy(rest_array, rest_array + size, array_ + GetSize());
  // IncreaseSize(size);
// }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealFirst(MappingType * value) -> bool {
  if (GetSize() - 1 < GetSize() < (GetMaxSize() + 1) / 2 - 1) { return false; }
  std::copy(array_ + 1, array_ + GetSize() + 1, array_);
  IncreaseSize(-1);
  *value = array_[0];
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealLast(MappingType * value) -> bool {
  if (GetSize() - 1 < GetSize() < (GetMaxSize() + 1) / 2 - 1) { return false; }
  *value = array_[GetSize()];
  IncreaseSize(-1);
  return true;
}

/**
 * @brief 
 * 
 * @param key 
 * @param comparator 
 * @return int 
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Search(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 1;
  int right = GetSize();

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
  
  return 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchPosition(page_id_t page_id) const -> int {
  int left = 0;
  int right = GetSize();

  while (left <= right) {
    int mid = (left + right) / 2;
    if (array_[mid].second == page_id) {
      return mid;
    }
    if (array_[mid].second < page_id) {
      left = mid + 1;
    } 
    else {
      right = mid - 1;
    }
  }
  
  return -1;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
