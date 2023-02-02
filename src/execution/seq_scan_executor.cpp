//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "storage/page/header_page.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  auto txn = exec_ctx_->GetTransaction();
  auto lm = exec_ctx_->GetLockManager();
  // acquire the lock
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn->IsTableIntentionExclusiveLocked(table_info_->oid_)) {
      if (!lm->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_)) {
        throw Exception("Execute Error!!!");
      }
    } else {
      if (!lm->LockTable(txn, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, table_info_->oid_)) {
        throw Exception("Execute Error!!!");
      }
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();
  auto lm = exec_ctx_->GetLockManager();
  if (table_iter_ == table_info_->table_->End()) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lm->UnlockTable(txn, table_info_->oid_);
    }
    return false;
  }
  /* == lock row == */
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    // if (!txn->IsRowExclusiveLocked(table_info_->oid_, table_iter_->GetRid())) {
    if (!lm->LockRow(txn, LockManager::LockMode::SHARED, table_info_->oid_, table_iter_->GetRid())) {
      throw Exception("Execute Error!!!");
    }
    // }
  }
  *tuple = *table_iter_;
  *rid = tuple->GetRid();
  /* == unlock row == */
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    lm->UnlockRow(txn, table_info_->oid_, table_iter_->GetRid());
  }

  table_iter_++;
  return true;
}
}  // namespace bustub
