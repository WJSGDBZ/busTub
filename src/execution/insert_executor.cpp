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
#include "concurrency/lock_manager.h"
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
      lock_manager_(exec_ctx_->GetLockManager()),
      transaction_manager_(exec_ctx->GetTransactionManager()),
      child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  Transaction *cur_transaction = exec_ctx_->GetTransaction();
  lock_manager_->LockTable(cur_transaction, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  int32_t cnt = 0;
  Transaction *cur_transaction = exec_ctx_->GetTransaction();
  while (true) {
    Tuple ch_tuple;
    RID ch_rid;
    if (!child_executor_->Next(&ch_tuple, &ch_rid)) {
      break;
    }

    // insert into heapTable
    auto result = table_info_->table_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, ch_tuple, lock_manager_,
                                                   cur_transaction, plan_->TableOid());
    BUSTUB_ENSURE(result.has_value(), "Fail to InsertExecutor InsertTuple");
    RID new_rid = result.value();
    // record transaction write set for abort safty
    TableWriteRecord w_record{plan_->TableOid(), new_rid, table_info_->table_.get()};
    w_record.wtype_ = WType::INSERT;
    cur_transaction->AppendTableWriteRecord(w_record);

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
