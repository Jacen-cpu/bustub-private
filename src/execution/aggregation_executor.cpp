//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/logger.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_({}) {}

void AggregationExecutor::Init() {
  // init the child executor
  child_->Init();
  // init the aggreation hash table
  Tuple tup{};
  RID tmp_rid{};

  // anyway insert without check
  if (child_->Next(&tup, &tmp_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tup), MakeAggregateValue(&tup));
  } else {
    if (plan_->GetGroupBys().empty()) {
      aht_.InsertCombine(MakeAggregateKey(&tup), {std::vector<Value>{ValueFactory::GetNullValueByType(TypeId::INTEGER)}});
    }
  }

  while (child_->Next(&tup, &tmp_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tup), MakeAggregateValue(&tup));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  auto group_vec = aht_iterator_.Key().group_bys_;
  auto aggr_vec = aht_iterator_.Val().aggregates_;
  group_vec.insert(group_vec.end(), aggr_vec.begin(), aggr_vec.end());
  *tuple = Tuple{group_vec, &GetOutputSchema()};
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
