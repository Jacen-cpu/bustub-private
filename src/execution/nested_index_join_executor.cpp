//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  child_executor_->Init();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple out_tup{};
  RID out_rid{};
  // find left tup
  while (true) {
    if (!child_executor_->Next(&out_tup, &out_rid)) {
      return false;
    }
    Value key = plan_->KeyPredicate()->Evaluate(&out_tup, child_executor_->GetOutputSchema());
    std::vector<RID> result{};
    tree_->ScanKey(Tuple{{key}, &index_info_->key_schema_}, &result, exec_ctx_->GetTransaction());
    LOG_DEBUG("result size is %zu", result.size());

    // find the tuple
    if (!result.empty()) {
      Tuple inner_tup{};
      table_info_->table_->GetTuple(result[0], &inner_tup, exec_ctx_->GetTransaction());
      std::vector<Value> values;
      values.reserve(plan_->OutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(out_tup.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
        values.emplace_back(inner_tup.GetValue(&plan_->InnerTableSchema(), i));
      }
      *tuple = Tuple{values, &plan_->OutputSchema()};
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      values.reserve(plan_->OutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(out_tup.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (auto &col : plan_->InnerTableSchema().GetColumns()) {
        values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = Tuple{values, &plan_->OutputSchema()};
      return true;
    }
  }
}

}  // namespace bustub
