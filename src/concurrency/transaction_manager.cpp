//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"

namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  for (auto write_set = (*txn->GetWriteSet()).rbegin(); write_set != (*txn->GetWriteSet()).rend(); write_set++) {
    switch (write_set->wtype_) {
      case WType::INSERT:
        write_set->table_heap_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, write_set->rid_);
        break;
      case WType::DELETE:
        write_set->table_heap_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, false}, write_set->rid_);
        // TODO(jun): I think some bugs there if update tuple meta directly
        // auto tuple_info = write_set.table_heap_->GetTuple(write_set.rid_);
        // write_set.table_heap_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, tuple_info.second);
        break;
      case WType::UPDATE:
        break;
    }
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
