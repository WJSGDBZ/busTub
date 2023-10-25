#include <memory>
#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  children.reserve(children.size());
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimize_plan = plan->CloneWithChildren(std::move(children));

  if (plan->GetType() == PlanType::Limit) {
    const auto *limit_plan = dynamic_cast<const LimitPlanNode *>(optimize_plan.get());
    BUSTUB_ENSURE(limit_plan->GetChildren().size() == 1, "Limit plan have two child? so weird");
    if (plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      const SortPlanNode *sort_plan = dynamic_cast<const SortPlanNode *>(optimize_plan->GetChildAt(0).get());
      BUSTUB_ENSURE(sort_plan->GetChildren().size() == 1, "Sort plan have two child? so weird");
      // TopNPlanNode(SchemaRef output, AbstractPlanNodeRef child,
      //         std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys, std::size_t n)
      return std::make_shared<TopNPlanNode>(sort_plan->output_schema_, sort_plan->GetChildAt(0),
                                            sort_plan->GetOrderBy(), limit_plan->GetLimit());
    }
  }

  return optimize_plan;
}

}  // namespace bustub
