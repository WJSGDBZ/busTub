//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/macros.h"
#include "execution/executors/update_executor.h"
#include "type/type.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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

    // update tuple in heapTable (delete and insert)
    table_info_->table_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, ch_rid);

    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &iter : plan_->target_expressions_) {
      values.emplace_back(iter->Evaluate(&ch_tuple, table_info_->schema_));
    }

    Tuple new_tuple = {values, &table_info_->schema_};
    auto result = table_info_->table_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple);
    BUSTUB_ENSURE(result.has_value(), "Fail to UpdateExecutor InsertTuple");
    RID new_rid = result.value();

    // update index
    std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto *info : index_info) {
      Schema *schema = info->index_->GetKeySchema();

      info->index_->DeleteEntry(ch_tuple.KeyFromTuple(table_info_->schema_, *schema, info->index_->GetKeyAttrs()),
                                ch_rid, exec_ctx_->GetTransaction());

      info->index_->InsertEntry(new_tuple.KeyFromTuple(table_info_->schema_, *schema, info->index_->GetKeyAttrs()),
                                new_rid, exec_ctx_->GetTransaction());
    }

    cnt++;
  }

  std::vector<Value> value{{INTEGER, cnt}};
  *tuple = Tuple{value, &GetOutputSchema()};
  done_ = true;

  return true;
}

}  // namespace bustub
