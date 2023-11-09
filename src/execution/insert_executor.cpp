//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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

    // insert into heapTable
    auto result = table_info_->table_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, ch_tuple);
    BUSTUB_ENSURE(result.has_value(), "Fail to InsertExecutor InsertTuple");
    RID new_rid = result.value();

    // update index
    std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto *info : index_info) {
      // std::cout << std::endl;
      // std::cout << "update index" << info->name_ << std::endl;
      Schema *schema = info->index_->GetKeySchema();

      // uint32_t num = schema->GetColumnCount();
      // std::cout << "schema count is " << num << std::endl;

      info->index_->InsertEntry(ch_tuple.KeyFromTuple(table_info_->schema_, *schema, info->index_->GetKeyAttrs()),
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
