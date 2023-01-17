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
#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/values_plan.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), is_fin_(false) {}

void InsertExecutor::Init() { table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid()); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("execute next");
  if (is_fin_) {
    return false;
  }
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto schema_cols = table_info_->schema_.GetColumns();
  int32_t insert_num = 0;
  auto child_plan = plan_->GetChildPlan();
  LOG_DEBUG("Child Plan Type is %d", child_plan->GetType());

  if (child_plan->GetType() == PlanType::Values) {
    auto value_plan = std::dynamic_pointer_cast<const ValuesPlanNode>(child_plan);
    for (const auto &exprs : value_plan->GetValues()) {
      if (exprs.size() != schema_cols.size()) {
        return false;
      }
      std::vector<Value> tup_vals;
      for (unsigned i = 0; i < exprs.size(); ++i) {
        if (exprs[i]->GetReturnType() != schema_cols[i].GetType()) {
          return false;
        }
        auto val = exprs[i]->Evaluate(nullptr, table_info_->schema_);
        tup_vals.push_back(val);
      }
      Tuple tup(tup_vals, &table_info_->schema_);
      RID tmp_rid;
      table_info_->table_->InsertTuple(tup, &tmp_rid, exec_ctx_->GetTransaction());
      // update the index
      for (auto index_info : table_indexes) {
        auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
        // construct key attrs
        std::vector<uint32_t> key_attrs{};
        for (const auto &col : index_info->key_schema_.GetColumns()) {
          key_attrs.push_back(table_info_->schema_.GetColIdx(col.GetName()));
        }
        // insert key into b+ tree
        tree->InsertEntry(tup.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs), tmp_rid,
                          exec_ctx_->GetTransaction());
      }
      insert_num++;
    }
    is_fin_ = true;
  } else {
    auto executor = ExecutorFactory::CreateExecutor(exec_ctx_, child_plan);
    executor->Init();
    Tuple tup;
    RID tmp_rid;
    while (executor->Next(&tup, &tmp_rid)) {
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
  }
  *tuple = Tuple{std::vector<Value>{Value{TypeId::INTEGER, insert_num}}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
