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
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *leaf, BufferPoolManager *bpm, int cur_pos)
    : leaf_(leaf), cur_pos_(cur_pos), buffer_pool_manager_(bpm) {
  cur_leaf_size_ = leaf->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_->IsLast() && cur_pos_ == cur_leaf_size_ - 1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_->MappingAt(cur_pos_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (cur_pos_ < cur_leaf_size_ - 1 || IsEnd()) {
    cur_pos_++;
  } else {
    cur_pos_ = 0;
    page_id_t cur_id = leaf_->GetPageId();
    page_id_t next_id = leaf_->GetNextPageId();
    leaf_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_id));
    buffer_pool_manager_->UnpinPage(cur_id, false);
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
