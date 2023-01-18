//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cassert>
#include <cstdint>
#include <iostream>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/logger.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  right_executor_->Init();
  left_executor_->Init();
  is_first_ = true;
  has_find_ = false;
}

void NestedLoopJoinExecutor::ConcatTuple(Tuple *right_tup, Tuple *out_tup) {
  std::vector<Value> values;
  values.reserve(plan_->OutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.emplace_back(left_tup_.GetValue(&left_executor_->GetOutputSchema(), i));
  }
  if (right_tup == nullptr) {
    for (auto & col : right_executor_->GetOutputSchema().GetColumns()) {
      values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
    }
  } else {
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.emplace_back(right_tup->GetValue(&right_executor_->GetOutputSchema(), i));
    }
  } 
  *out_tup = Tuple{values, &plan_->OutputSchema()};
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tup{};
  RID right_rid{};
  while (true) {
    const auto status = right_executor_->Next(&right_tup, &right_rid);
    if (!status || is_first_) {
      // address the left join case
      if (plan_->GetJoinType() == JoinType::LEFT && !has_find_ && !is_first_) {
        ConcatTuple(nullptr, tuple);
        has_find_ = true;
        return true;
      }
      if (!left_executor_->Next(&left_tup_, &left_rid_)) {
        return false;
      }
      has_find_ = false;
      is_first_ = false;
      // re init the right executor
      right_executor_->Init();
      // if (plan_)
      const auto n_st = right_executor_->Next(&right_tup, &right_rid);
      if (plan_->GetJoinType() == JoinType::LEFT && !n_st) {
        has_find_ = true;
        ConcatTuple(nullptr, tuple);
        return true;
      }
      if (!n_st) { return false; }
    }
    
    auto value = plan_->Predicate().EvaluateJoin(&left_tup_, left_executor_->GetOutputSchema(), &right_tup,
                                                 right_executor_->GetOutputSchema());
                                          
    // concat the two tuple
    if (!value.IsNull() && value.GetAs<bool>()) {
      ConcatTuple(&right_tup, tuple);
      has_find_ = true;
      return true;
    }
  }
}

}  // namespace bustub
