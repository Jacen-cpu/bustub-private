//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
    index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_); 
    tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
    index_iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (index_iter_ == tree_->GetEndIterator()) { return false; }
    auto mapping_value = *index_iter_;
    ++index_iter_;
    return table_info_->table_->GetTuple(mapping_value.second, tuple, exec_ctx_->GetTransaction());
}

}  // namespace bustub
