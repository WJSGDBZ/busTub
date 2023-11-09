//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <vector>
#include "catalog/schema.h"
#include "storage/table/table_iterator.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_iterator_(table_info_->table_->MakeIterator()) {}

void SeqScanExecutor::Init() {
  const Schema schema = GetOutputSchema();
  cnt_ = 0;

  while (!done_ && !table_iterator_.IsEnd()) {
    auto tuple_info = table_iterator_.GetTuple();
    if (tuple_info.first.is_deleted_) {
      ++table_iterator_;
      continue;
    }

    tuple_info_.emplace_back(std::move(tuple_info.second), table_iterator_.GetRID());
    ++table_iterator_;
  }

  done_ = true;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cnt_ == tuple_info_.size()) {
    return false;
  }

  *tuple = tuple_info_[cnt_].first;
  *rid = tuple_info_[cnt_].second;
  cnt_++;

  return true;
}

}  // namespace bustub
