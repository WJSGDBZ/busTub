#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ParserLogicExpression(const AbstractExpressionRef &expr, std::vector<ComparisonExpression *> &compare_exprs)
    -> bool {
  if (auto *compare_expr = dynamic_cast<ComparisonExpression *>(expr.get()); compare_expr != nullptr) {
    compare_exprs.push_back(compare_expr);
    return true;
  }

  if (const auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get()); logic_expr != nullptr) {
    for (const auto &child_expr : logic_expr->GetChildren()) {
      if (!ParserLogicExpression(child_expr, compare_exprs)) {
        return false;
      }
    }

    return true;
  }

  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr> . .
  std::vector<AbstractPlanNodeRef> children;
  children.reserve(children.size());
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimize_plan = plan->CloneWithChildren(std::move(children));

  if (plan->GetType() == PlanType::NestedLoopJoin) {
    // translate nest loop join to hash join
    const auto &nest_loop_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimize_plan);
    BUSTUB_ENSURE(nest_loop_plan.GetChildren().size() == 2, "NestedLoopJoinPlanNode's children is not 2? so weird!");

    std::vector<ComparisonExpression *> compare_exprs;
    if (!ParserLogicExpression(nest_loop_plan.Predicate(), compare_exprs)) {
      return optimize_plan;
    }

    bool all_equal = true;
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    for (const auto &compare_expr : compare_exprs) {
      if (compare_expr->comp_type_ != ComparisonType::Equal) {
        all_equal = false;
        break;
      }

      const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(compare_expr->children_[0].get());
      const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(compare_expr->children_[1].get());
      if (left_expr != nullptr && right_expr != nullptr) {
        switch (left_expr->GetTupleIdx()) {
          case 0:
            left_key_expressions.emplace_back(
                std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
            right_key_expressions.emplace_back(
                std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
            break;
          case 1:
            left_key_expressions.emplace_back(
                std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
            right_key_expressions.emplace_back(
                std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
            break;
        }
      } else {
        throw bustub::NotImplementedException(fmt::format("ArithmeticExpression not support now"));
      }
    }

    if (all_equal) {
      // HashJoinPlanNode(SchemaRef output_schema, AbstractPlanNodeRef left, AbstractPlanNodeRef right,
      //            std::vector<AbstractExpressionRef> left_key_expressions,
      //            std::vector<AbstractExpressionRef> right_key_expressions, JoinType join_type)
      return std::make_shared<HashJoinPlanNode>(nest_loop_plan.output_schema_, nest_loop_plan.GetLeftPlan(),
                                                nest_loop_plan.GetRightPlan(), std::move(left_key_expressions),
                                                std::move(right_key_expressions), nest_loop_plan.GetJoinType());
    }
  }

  return optimize_plan;
}

}  // namespace bustub
