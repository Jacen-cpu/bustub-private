//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "common/logger.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
  exps_iter_ = plan_->target_expressions_.begin();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("&&&&&&&&&&&&&&&&&&&&&&&&&update tup");
  Tuple tup;
  RID tmp_rid;
  if (!child_executor_->Next(&tup, &tmp_rid)) {
    return false;
  }
  auto value = (*exps_iter_)->Evaluate(&tup, plan_->OutputSchema());
  LOG_DEBUG("value is %s", value.ToString().c_str());
  LOG_DEBUG("&&&&&&&&&&&&&&&&&&&&&&&&&update tup is %s", tup.ToString(&plan_->OutputSchema()).c_str());
  table_info_->table_->UpdateTuple(tup, tmp_rid, exec_ctx_->GetTransaction());
  *tuple = tup;
  return true;
}

// auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
// const auto &update_attrs = plan_->GetUpdateAttr();
// Schema schema = table_info_->schema_;
// uint32_t col_count = schema.GetColumnCount();
// std::vector<Value> values;
// for (uint32_t idx = 0; idx < col_count; idx++) {
// if (update_attrs.find(idx) == update_attrs.cend()) {
// values.emplace_back(src_tuple.GetValue(&schema, idx));
// } else {
// const UpdateInfo info = update_attrs.at(idx);
// Value val = src_tuple.GetValue(&schema, idx);
// switch (info.type_) {
// case UpdateType::Add:
// values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
// break;
// case UpdateType::Set:
// values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
// break;
// }
// }
// }
// return Tuple{values, &schema};
// }

}  // namespace bustub
