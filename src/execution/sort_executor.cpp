#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <utility>
#include "execution/expressions/column_value_expression.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  while (true) {
    Tuple ch_tuple;
    RID ch_rid;
    if (!child_executor_->Next(&ch_tuple, &ch_rid)) {
      break;
    }
    sorted_tuples_info_.emplace_back(std::move(ch_tuple), ch_rid);
  }

  const auto &order_by = plan_->GetOrderBy();
  for (auto iter = order_by.rbegin(); iter != order_by.rend(); iter++) {
    std::stable_sort(sorted_tuples_info_.begin(), sorted_tuples_info_.end(),
                     [&iter, this](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) {
                       Value value_a = iter->second->Evaluate(&a.first, plan_->OutputSchema());
                       Value value_b = iter->second->Evaluate(&b.first, plan_->OutputSchema());
                       switch (iter->first) {
                         case OrderByType::DEFAULT:
                         case OrderByType::ASC:
                           return value_a.CompareLessThan(value_b) == CmpBool::CmpTrue;
                         case OrderByType::DESC:
                           return value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue;
                         case OrderByType::INVALID:
                           return false;
                       }
                     });
  }
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cnt_ != sorted_tuples_info_.size()) {
    *tuple = sorted_tuples_info_[cnt_].first;
    *rid = sorted_tuples_info_[cnt_].second;

    cnt_++;
    return true;
  }

  return false;
}

}  // namespace bustub
