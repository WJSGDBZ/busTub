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

#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();

  while (true) {
    Tuple ch_tuple;
    RID ch_rid;
    if (!child_executor_->Next(&ch_tuple, &ch_rid)) {
      break;
    }
    aht_.InsertCombine(MakeAggregateKey(&ch_tuple), MakeAggregateValue(&ch_tuple));
  }

  if (aht_.Begin() == aht_.End()) {  // empty table
    empty_table_ = true;
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (empty_table_) {
    std::vector<Value> values;
    if (!plan_->group_bys_.empty()) {
      return false;
    }

    *tuple = {std::move(aht_.GenerateInitialAggregateValue().aggregates_), &GetOutputSchema()};
    empty_table_ = false;
    return true;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values;
  values.reserve(aht_iterator_.Val().aggregates_.size() + aht_iterator_.Key().group_bys_.size());
  if (!plan_->group_bys_.empty()) {
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  }
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());

  *tuple = {values, &GetOutputSchema()};

  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
