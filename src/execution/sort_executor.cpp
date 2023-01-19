#include "execution/executors/sort_executor.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "execution/executor_context.h"
#include "storage/index/generic_key.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tup{};
  RID tmp_rid{};
  // insert all fetched tuple into the vector
  child_executor_->Init();
  // don't forget clear the vector for repeat test!
  all_tuples_.clear();
  while (child_executor_->Next(&tup, &tmp_rid)) {
    all_tuples_.emplace_back(tup);
  }
  std::sort(all_tuples_.begin(), all_tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      Value v_a = expr->Evaluate(&a, child_executor_->GetOutputSchema());
      Value v_b = expr->Evaluate(&b, child_executor_->GetOutputSchema());
      switch (order_by_type) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (v_a.CompareEquals(v_b) == CmpBool::CmpTrue) {
            break;
          }
          return v_a.CompareLessThan(v_b) == CmpBool::CmpTrue;
        case OrderByType::DESC:
          if (v_a.CompareEquals(v_b) == CmpBool::CmpTrue) {
            break;
          }
          return v_a.CompareGreaterThan(v_b) == CmpBool::CmpTrue;
          break;
      }
    }
    return true;
  });
  all_tuples_iter_ = all_tuples_.begin();
}

void SortExecutor::ExtractSortKeys(Tuple *key, Tuple *tup) {
  std::vector<Value> key_values;
  key_values.reserve(plan_->GetOrderBy().size());
  for (auto &order : plan_->GetOrderBy()) {
    key_values.emplace_back(order.second->Evaluate(tup, child_executor_->GetOutputSchema()));
  }
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (all_tuples_iter_ == all_tuples_.end()) {
    return false;
  }
  *tuple = *all_tuples_iter_;
  *rid = tuple->GetRid();
  ++all_tuples_iter_;
  return true;
}

}  // namespace bustub
