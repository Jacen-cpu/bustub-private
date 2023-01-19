#include "execution/executors/topn_executor.h"
#include <memory>
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  top_tuple_.clear();

  auto key_comp = [this](const Tuple &a, const Tuple &b) {
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
          return v_a.CompareGreaterThan(v_b) == CmpBool::CmpTrue;
        case OrderByType::DESC:
          if (v_a.CompareEquals(v_b) == CmpBool::CmpTrue) {
            break;
          }
          return v_a.CompareLessThan(v_b) == CmpBool::CmpTrue;
          break;
      }
    }
    return true;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(key_comp)> priority_tuples(key_comp);
  Tuple tup{};
  RID tmp_rid{};
  while (child_executor_->Next(&tup, &tmp_rid)) {
    priority_tuples.push(tup);
  }

  top_tuple_.reserve(plan_->GetN());
  for (size_t i = 0; i < plan_->GetN() && !priority_tuples.empty(); ++i) {
    top_tuple_.emplace_back(priority_tuples.top());
    priority_tuples.pop();
  }
  top_tuple_iter_ = top_tuple_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (top_tuple_iter_ == top_tuple_.end()) {
    return false;
  }
  *tuple = *top_tuple_iter_;
  *rid = tuple->GetRid();
  ++top_tuple_iter_;
  return true;
}

}  // namespace bustub
