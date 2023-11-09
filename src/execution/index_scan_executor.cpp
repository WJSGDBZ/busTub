//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "catalog/schema.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)),
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      index_iterator_(
          dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  cnt_ = 0;

  while (!done_ && !index_iterator_.IsEnd()) {
    // get rid by index
    RID rid = (*index_iterator_).second;

    // get tuple by rid
    auto tuple_info = table_info_->table_->GetTuple(rid);
    if (tuple_info.first.is_deleted_) {
      ++index_iterator_;
      continue;
    }

    tuple_info_.emplace_back(std::move(tuple_info.second), rid);
    ++index_iterator_;
  }

  done_ = true;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cnt_ == tuple_info_.size()) {
    return false;
  }

  *tuple = tuple_info_[cnt_].first;
  *rid = tuple_info_[cnt_].second;
  cnt_++;

  return true;
}

}  // namespace bustub
