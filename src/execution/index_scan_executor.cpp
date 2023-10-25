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

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!index_iterator_.IsEnd()) {
    // get rid by index
    *rid = (*index_iterator_).second;

    // get tuple by rid
    auto tuple_info = table_info_->table_->GetTuple(*rid);
    if (tuple_info.first.is_deleted_) {
      ++index_iterator_;
      continue;
    }

    *tuple = tuple_info.second;

    ++index_iterator_;
    return true;
  }

  return false;
}

}  // namespace bustub
