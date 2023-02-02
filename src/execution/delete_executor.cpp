//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/table_heap.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), is_fin_(false) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto txn = exec_ctx_->GetTransaction();
  auto lm = exec_ctx_->GetLockManager();
  /* == Lock Table == */
  try {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      lm->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    }
  } catch (TransactionAbortException &e) {
    LOG_INFO("%s", e.GetInfo().c_str());
    throw e;
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_fin_) {
    return false;
  }
  Tuple child_tuple{};
  RID tmp_rid{};
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  int32_t delete_num = 0;
  auto txn = exec_ctx_->GetTransaction();
  auto lm = exec_ctx_->GetLockManager();

  while (true) {
    try {
      if (!child_executor_->Next(&child_tuple, &tmp_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteExecutor:child execute error.");
      return false;
    }

    /* == Lock Row == */
    lm->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, tmp_rid);

    if (!table_info_->table_->MarkDelete(tmp_rid, txn)) {
      return false;
    }
    // update the index
    for (auto index_info : table_indexes) {
      auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
      // construct key attrs
      std::vector<uint32_t> key_attrs{};
      for (const auto &col : index_info->key_schema_.GetColumns()) {
        key_attrs.push_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      tree->DeleteEntry(child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs), tmp_rid,
                        txn);
    }
    delete_num++;
  }
  is_fin_ = true;
  *tuple = Tuple{std::vector<Value>{Value{TypeId::INTEGER, delete_num}}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
