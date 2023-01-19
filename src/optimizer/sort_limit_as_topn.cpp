#include <memory>
#include "common/logger.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (plan->GetChildren().empty()) { return plan; }

  if (plan->GetType() == PlanType::Limit && plan->GetChildAt(0)->GetType() == PlanType::Sort) {
    auto limit_plan = std::dynamic_pointer_cast<const LimitPlanNode>(plan);
    auto sort_plan = std::dynamic_pointer_cast<const SortPlanNode>(plan->GetChildAt(0));
    auto top_n_plan = std::make_shared<const TopNPlanNode>(sort_plan->output_schema_, sort_plan->GetChildPlan(),
                                                            sort_plan->GetOrderBy(), limit_plan->GetLimit());
    
    LOG_DEBUG("opt success!");
    return OptimizeSortLimitAsTopN(top_n_plan);
  } 

  std::vector<AbstractPlanNodeRef> new_child_plan{};
  for (const auto &old_child_plan : plan->GetChildren()) {
    new_child_plan.emplace_back(OptimizeSortLimitAsTopN(old_child_plan));
  }
  return plan->CloneWithChildren(new_child_plan);
}

}  // namespace bustub
