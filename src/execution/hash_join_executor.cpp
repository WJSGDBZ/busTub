//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  ht_.clear();
  tuples_.clear();
  cnt_ = 0;
  while (true) {
    Tuple right_tuple;
    RID right_rid;
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      break;
    }

    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(&right_tuple, right_executor_->GetOutputSchema()));
    }

    ht_[{keys}].push_back(std::move(right_tuple));
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  size_t cnt = left_executor_->GetOutputSchema().GetColumnCount() + right_executor_->GetOutputSchema().GetColumnCount();
  Schema join_schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());
  while (true) {
    if (cnt_ != tuples_.size()) {
      *tuple = std::move(tuples_[cnt_++]);
      return true;
    }

    Tuple left_tuple;
    RID left_rid;
    if (!left_executor_->Next(&left_tuple, &left_rid)) {
      break;
    }

    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(&left_tuple, left_executor_->GetOutputSchema()));
    }

    if (ht_.find({keys}) == ht_.end()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        values.reserve(cnt);

        // no match, fill right tuple NULL Value
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = {std::move(values), &join_schema};
        return true;
      }

      continue;
    }

    tuples_.clear();
    cnt_ = 0;
    for (auto &iter : ht_[{keys}]) {
      std::vector<Value> values;
      values.reserve(cnt);

      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(iter.GetValue(&right_executor_->GetOutputSchema(), i));
      }

      tuples_.emplace_back(std::move(values), &join_schema);
    }

    *tuple = std::move(tuples_[cnt_++]);
    return true;
  }

  return false;
}

}  // namespace bustub
