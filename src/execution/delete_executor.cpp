//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  int32_t cnt = 0;
  while (true) {
    Tuple ch_tuple;
    RID ch_rid;
    if (!child_executor_->Next(&ch_tuple, &ch_rid)) {
      break;
    }

    // delete tuple in heapTable (set field "is_deleted_" to true)
    table_info_->table_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, ch_rid);

    // update index
    std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto *info : index_info) {
      Schema *schema = info->index_->GetKeySchema();
      info->index_->DeleteEntry(ch_tuple.KeyFromTuple(table_info_->schema_, *schema, info->index_->GetKeyAttrs()),
                                ch_rid, exec_ctx_->GetTransaction());
    }

    cnt++;
  }

  std::vector<Value> value{{INTEGER, cnt}};
  *tuple = Tuple{value, &GetOutputSchema()};
  done_ = true;

  return true;
}

}  // namespace bustub
