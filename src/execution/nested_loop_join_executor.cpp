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
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  while (true) {
    Tuple right_tuple;
    RID right_rid;

    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      break;
    }

    right_tuple_.push_back(std::move(right_tuple));
  }

  while (true) {
    Tuple left_tuple;
    RID left_rid;
    if (!left_executor_->Next(&left_tuple, &left_rid)) {
      break;
    }

    left_tuple_.push_back(std::move(left_tuple));
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_cursor_ < left_tuple_.size()) {
    size_t left_tuple_cnt = left_executor_->GetOutputSchema().GetColumnCount();
    size_t right_tuple_cnt = right_executor_->GetOutputSchema().GetColumnCount();
    Schema join_schema = bustub::NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());

    std::vector<Value> values;
    size_t cnt = left_tuple_cnt + right_tuple_cnt;
    values.reserve(cnt);
    while (right_cursor_ < right_tuple_.size()) {
      auto match = plan_->Predicate()->EvaluateJoin(&left_tuple_[left_cursor_], left_executor_->GetOutputSchema(),
                                                    &right_tuple_[right_cursor_], right_executor_->GetOutputSchema());
      if (!match.IsNull() && match.GetAs<bool>()) {
        // only combine matched
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_[left_cursor_].GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple_[right_cursor_].GetValue(&right_executor_->GetOutputSchema(), i));
        }

        *tuple = {values, &join_schema};
        right_cursor_++;
        is_match_ = true;
        return true;
      }
      right_cursor_++;
    }

    right_cursor_ = 0;  // refresh
    right_executor_->Init();
    if (!is_match_ && plan_->GetJoinType() == JoinType::LEFT) {
      // others, fill right tuple NULL Value
      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_[left_cursor_].GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = {values, &join_schema};
      is_match_ = false;
      left_cursor_++;
      return true;
    }

    is_match_ = false;
    left_cursor_++;
  }

  return false;
}
}  // namespace bustub
