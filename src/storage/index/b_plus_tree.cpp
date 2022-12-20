#include <cassert>
#include <new>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
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

/*
 * Helper function to find the leaf node page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *{
  auto curr_node_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  
  while (!curr_node_page->IsLeafPage()) {
    // guide by the internal page (key) [...).
    // do some comparations
    auto internal_node_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(curr_node_page);
    page_id_t next_page_id = [&]() -> page_id_t {
      for (int i = 1; i <= internal_node_page->GetSize(); ++i) {
        KeyType cur_key = internal_node_page->KeyAt(i);
        if (comparator_(cur_key, key) == 1) { 
          return internal_node_page->ValueAt(i - 1);
        }
      }
      return internal_node_page->ValueAt(internal_node_page->GetSize());
    }(); // find the next page id (down), (maybe lambda is good)

    curr_node_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }
  
  return reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(curr_node_page);
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
  if (IsEmpty()) { return false; }
  // fetch the page
  auto leaf_node_page = FindLeafPage(key);
  for (int i = 0; i < leaf_node_page->GetSize(); ++i) {
    KeyType cur_key = leaf_node_page->KeyAt(i);
    if (comparator_(cur_key, key) == 0) {
      result->push_back(leaf_node_page->ValueAt(i));
      return true;
    }
  }
  
  return false;
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
  if (IsEmpty()) {
    page_id_t new_root_page_id;
    auto new_root = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_root_page_id)
    );
    UpdateRootPageId(0);
    root_page_id_ = new_root_page_id;
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_root->Insert(key, value, comparator_);
    return true;
  }
   
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * leaf_node_page = FindLeafPage(key);
  bool success = leaf_node_page->Insert(key, value, comparator_);
  if (!success) {
    // duplicate!!
    return false;
  }  
  // check if needing splitting the node
  if (leaf_node_page->NeedSplit()) {
    SplitLeaf(leaf_node_page);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * over_node) {
  int size = over_node->GetSize();
  int mid_index = over_node->GetMaxSize() / 2;
  KeyType mid_key = over_node->KeyAt(mid_index);

  /* == get parent node == */ 
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * parent_node = nullptr;
  
  // create new leaf node page
  page_id_t new_leaf_page_id; 
  auto new_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
    buffer_pool_manager_->NewPage(&new_leaf_page_id)
  );

  if (!over_node->IsRootPage()) {
    new_leaf->Init(new_leaf_page_id, over_node->GetParentPageId(), leaf_max_size_);
  } else {
    // create new root node page
    page_id_t new_root_page_id;
    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_root_page_id)
    );
    UpdateRootPageId(0);
    root_page_id_ = new_root_page_id;
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetFirstPoint(over_node->GetPageId());
    
    over_node->SetParentPageId(root_page_id_);
    new_leaf->Init(new_leaf_page_id, root_page_id_, leaf_max_size_);
    
    parent_node = new_root;
  }

  /* == Address the parent level == */
  if (parent_node == nullptr) {
    parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(
        over_node->GetParentPageId())->GetData()
    );
  }
  parent_node->Insert(mid_key, new_leaf_page_id, comparator_);

  /* == Address the leaf level == */
  new_leaf->SetNextPageId(over_node->GetNextPageId());
  over_node->SetNextPageId(new_leaf_page_id);
  
  auto left_arr = over_node->GetArray();
  auto right_arr = new_leaf->GetArray();
  /* address the right array */
  std::copy(left_arr + mid_index, left_arr + size, right_arr);
  new_leaf->IncreaseSize(size - mid_index);

  /* address the left array */
  over_node->IncreaseSize( -1 * (size - mid_index));

  /* == check if need to split the parent == */
  if (parent_node->NeedSplit()) {
    page_id_t new_page_id; 
    auto new_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_page_id)
    );
    new_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    SplitInternal(parent_node, new_node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * over_node,
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * new_internal) {
  int size = over_node->GetSize();
  int mid_index = over_node->GetMaxSize() / 2 + 1;
  KeyType mid_key = over_node->KeyAt(mid_index);
  page_id_t new_internal_page_id = new_internal->GetPageId();

  /* == get parent node == */
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * parent_node = nullptr;
  
  if (!over_node->IsRootPage()) {
    new_internal->SetParentPageId(over_node->GetParentPageId());
  } else {
    // create new root node page
    page_id_t new_root_page_id;
    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_root_page_id)
    );
    UpdateRootPageId(0);
    root_page_id_ = new_root_page_id;
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetFirstPoint(over_node->GetPageId());

    over_node->SetParentPageId(root_page_id_);
    new_internal->Init(new_internal_page_id, root_page_id_, internal_max_size_);

    parent_node = new_root;
  }

  /* == Address the parent level == */
  if (parent_node == nullptr) {
    parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(
        over_node->GetParentPageId())->GetData()
    );
  }
  parent_node->Insert(mid_key, new_internal_page_id, comparator_);
  
  /* == Address the low level (cut data into the new internal node.) == */ 
  auto left_arr = over_node->GetArray();
  auto right_arr = new_internal->GetArray();
  page_id_t mid_page_id = left_arr[mid_index].second;

  /* address the right array
    (we have to iterate it, because we want to update the ParentId for the child node.) */
  for (int i = 0; i < size - mid_index; ++i) {
    auto mapp_elem = left_arr[i + mid_index + 1];
    UpdateParentId(mapp_elem.second, new_internal_page_id);
    right_arr[i + 1] = mapp_elem;
  }
  UpdateParentId(mid_page_id, new_internal_page_id);
  new_internal->SetFirstPoint(mid_page_id);
  new_internal->IncreaseSize(size - mid_index);

  /* address the left array 
    (we don't have to clear the data, we can just decrease the size of the node.) */
  over_node->IncreaseSize( -1 * (size - mid_index + 1));

  if (parent_node->NeedSplit()) {
    page_id_t new_page_id; 
    auto new_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_page_id)
    );

    new_node->Init(new_internal_page_id, INVALID_PAGE_ID, internal_max_size_);
    SplitInternal(parent_node, new_node);
  }
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentId(page_id_t page_id, page_id_t p_page_id) {
    auto target_page = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager_->FetchPage(
        page_id)->GetData()
    );
    target_page->SetParentPageId(p_page_id);
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
  if (IsEmpty()) { return; }

  bool need_update = false;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * leaf_node_page = FindLeafPage(key);
  
  // fail to remove
  if (!leaf_node_page->Remove(key, comparator_, &need_update)) {
    return;
  }
  
  // success remove without distribute
  if (!leaf_node_page->NeedRedsb()) {
    /* check if we need update parent key */
    if (need_update) {
      UpdateParentKey(key, leaf_node_page->GetArray()[0].first, leaf_node_page->GetParentPageId());
    }
    return;
  }
  
  // TODO(gigalo): Here we go to address the Redistribution.
  /* steal (borrow) from the sibings */
  if (StealSibling(leaf_node_page, leaf_node_page->GetArray() + leaf_node_page->GetSize())) { return; }
  
  /* merge this leaf to its next page */

}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StealSibling(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * deleted_leaf, MappingType * value) -> bool {
  auto next_leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
    buffer_pool_manager_->FetchPage(
      deleted_leaf->GetNextPageId())->GetData()
  );

  if (!next_leaf_page->Steal(value)) {
    return false;
  }
  
  UpdateParentKey(value->first, next_leaf_page->GetArray()[0].first,next_leaf_page->GetParentPageId());
  return true;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentKey(const KeyType &old_key, const KeyType &new_key, page_id_t parent_id) {
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
    buffer_pool_manager_->FetchPage(
      parent_id)->GetData()
  );
  
  int index = parent_page->Search(old_key, comparator_);
  // assert for protect programming
  if (index != -1) {
    parent_page->GetArray()[index].first = new_key;
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
  return INDEXITERATOR_TYPE(); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
