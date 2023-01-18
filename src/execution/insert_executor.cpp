//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "execution/executors/insert_executor.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/values_plan.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), is_fin_(false) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("execute next");
  if (is_fin_) {
    return false;
  }
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  int32_t insert_num = 0;
  Tuple tup;
  RID tmp_rid;
  while (child_executor_->Next(&tup, &tmp_rid)) {
    table_info_->table_->InsertTuple(tup, &tmp_rid, exec_ctx_->GetTransaction());
    // update the index
    for (auto index_info : table_indexes) {
      auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
      // construct key attrs
      std::vector<uint32_t> key_attrs{};
      for (const auto &col : index_info->key_schema_.GetColumns()) {
        key_attrs.push_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      tree->InsertEntry(tup.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs), tmp_rid,
                        exec_ctx_->GetTransaction());
    }
    insert_num++;
  }
  is_fin_ = true;
  *tuple = Tuple{std::vector<Value>{Value{TypeId::INTEGER, insert_num}}, &GetOutputSchema()};
  // *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
