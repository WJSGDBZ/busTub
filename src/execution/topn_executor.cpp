#include "execution/executors/topn_executor.h"
#include <queue>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "execution/expressions/column_value_expression.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  const auto &order_by = plan_->GetOrderBy();
  Schema out_schema = plan_->OutputSchema();
  auto compare = [&order_by, &out_schema](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) {
    for (const auto &pair : order_by) {
      Value value_a = pair.second->Evaluate(&a.first, out_schema);
      Value value_b = pair.second->Evaluate(&b.first, out_schema);

      switch (pair.first) {
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return true;
          } else if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return false;
          } else if (value_a.CompareEquals(value_b) == CmpBool::CmpTrue) {
            continue;
          }
          break;
        case OrderByType::DESC:
          if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return true;
          } else if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return false;
          } else if (value_a.CompareEquals(value_b) == CmpBool::CmpTrue) {
            continue;
          }
          break;
        case OrderByType::INVALID:
          return false;
      }
    }

    return false;
  };

  std::priority_queue<std::pair<Tuple, RID>, std::vector<std::pair<Tuple, RID>>, decltype(compare)> que(compare);
  while (true) {
    Tuple ch_tuple;
    RID ch_rid;
    if (!child_executor_->Next(&ch_tuple, &ch_rid)) {
      break;
    }

    if (que.size() < plan_->GetN()) {
      // std::cout << "insert tuple " << ch_tuple.ToString(&plan_->OutputSchema()) << std::endl;
      que.emplace(std::move(ch_tuple), ch_rid);
    } else {
      if (Compare(que.top().first, ch_tuple, order_by, 0)) {
        // std::cout << "top is " <<  que.top().first.ToString(&plan_->OutputSchema()) << std::endl;
        // std::cout << "cur is" << ch_tuple.ToString(&plan_->OutputSchema()) << std::endl;
        // std::cout << "we need to pop" <<std::endl;
        que.pop();
        que.emplace(std::move(ch_tuple), ch_rid);
      }
    }
  }

  while (!que.empty()) {
    top_n_.push_back(que.top());
    que.pop();
  }

  for (auto iter = order_by.rbegin(); iter != order_by.rend(); iter++) {
    std::stable_sort(top_n_.begin(), top_n_.end(),
                     [&out_schema, &iter](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) {
                       Value value_a = iter->second->Evaluate(&a.first, out_schema);
                       Value value_b = iter->second->Evaluate(&b.first, out_schema);
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

auto TopNExecutor::Compare(const Tuple &top, const Tuple &cur,
                           const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by, size_t level)
    -> bool {
  if (level == order_by.size()) {
    return false;
  }

  auto expr = order_by[level].second;
  auto type = plan_->order_bys_[level].first;
  Value value_top = expr->Evaluate(&top, plan_->OutputSchema());
  Value value_cur = expr->Evaluate(&cur, plan_->OutputSchema());
  switch (type) {
    case OrderByType::DEFAULT:
    case OrderByType::ASC:
      if (value_cur.CompareLessThan(value_top) == CmpBool::CmpTrue) {
        return true;
      } else if (value_cur.CompareEquals(value_top) == CmpBool::CmpTrue) {
        return Compare(top, cur, order_by, level + 1);
      }
      break;
    case OrderByType::DESC:
      if (value_cur.CompareGreaterThan(value_top) == CmpBool::CmpTrue) {
        return true;
      } else if (value_cur.CompareEquals(value_top) == CmpBool::CmpTrue) {
        return Compare(top, cur, order_by, level + 1);
      }
    case OrderByType::INVALID:
      break;
  }

  return false;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cnt_ != top_n_.size()) {
    *tuple = top_n_[cnt_].first;
    *rid = top_n_[cnt_].second;

    cnt_++;
    return true;
  }

  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_n_.size(); }

}  // namespace bustub
