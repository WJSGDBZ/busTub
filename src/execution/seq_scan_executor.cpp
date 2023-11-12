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
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_iterator_(table_info_->table_->MakeEagerIterator()),
      lock_manager_(exec_ctx_->GetLockManager()),
      transaction_manager_(exec_ctx->GetTransactionManager()) {}

void SeqScanExecutor::Init() {
  cnt_ = 0;

  if (!done_) {
    CheckIfLockTable();
  }

  done_ = true;
}

auto SeqScanExecutor::IsVisible(TupleMeta &tuple_meta) -> bool {
  // Transaction *cur_transaction = exec_ctx_->GetTransaction();
  // bool other_txn_insert_ongoing = false;
  // bool other_txn_delete_ongoing = false;
  // switch (cur_transaction->GetIsolationLevel()) {
  //   case IsolationLevel::READ_UNCOMMITTED:
  //     break;
  //   case IsolationLevel::READ_COMMITTED:
  //   case IsolationLevel::REPEATABLE_READ:
  //     if(tuple_meta.insert_txn_id_ != INVALID_TXN_ID){
  //       other_txn_insert_ongoing = true;
  //     }
  //     if(tuple_meta.delete_txn_id_ != INVALID_TXN_ID){
  //       other_txn_delete_ongoing = true;
  //     }
  //     break;
  // }

  return !tuple_meta.is_deleted_;  // do we need to care about other txn after fetch lock?
}

auto SeqScanExecutor::CheckIfHoldHigherLockTable(LockManager::LockMode mode, table_oid_t oid) -> bool {
  Transaction *cur_transaction = exec_ctx_->GetTransaction();

  bool s_lock = cur_transaction->IsTableSharedLocked(oid);
  bool x_lock = cur_transaction->IsTableExclusiveLocked(oid);
  bool is_lock = cur_transaction->IsTableIntentionSharedLocked(oid);
  bool ix_lock = cur_transaction->IsTableIntentionExclusiveLocked(oid);
  bool six_lock = cur_transaction->IsTableSharedIntentionExclusiveLocked(oid);

  switch (cur_transaction->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
        return ix_lock || x_lock || six_lock;
      }
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      switch (mode) {
        case LockManager::LockMode::SHARED:
          return s_lock || x_lock || six_lock;
        case LockManager::LockMode::EXCLUSIVE:
          return x_lock;
        case LockManager::LockMode::INTENTION_SHARED:
          return is_lock || s_lock || x_lock || ix_lock || six_lock;
        case LockManager::LockMode::INTENTION_EXCLUSIVE:
          return ix_lock || x_lock || six_lock;
        case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
          return six_lock || x_lock;
      }
      break;
  }

  return false;
}

auto SeqScanExecutor::CheckIfLockTable() -> bool {
  Transaction *cur_transaction = exec_ctx_->GetTransaction();

  bool can_lock = true;
  bool is_lock = false;
  LockManager::LockMode mode = LockManager::LockMode::INTENTION_SHARED;
  if (exec_ctx_->IsDelete()) {
    mode = LockManager::LockMode::INTENTION_EXCLUSIVE;
  }

  if (CheckIfHoldHigherLockTable(mode, plan_->GetTableOid())) {  // already hold higher level lock
    return true;
  }

  switch (cur_transaction->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
        is_lock = true;
        can_lock = lock_manager_->LockTable(cur_transaction, mode, plan_->GetTableOid());
      }
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      is_lock = true;
      can_lock = lock_manager_->LockTable(cur_transaction, mode, plan_->GetTableOid());
      break;
  }

  if (!can_lock) {
    throw ExecutionException("SeqScanExecutor fail to lock Tuple");
  }

  return is_lock;
}

void SeqScanExecutor::CheckIfUnlockRow(bool force) {
  Transaction *cur_transaction = exec_ctx_->GetTransaction();
  if (force) {
    lock_manager_->UnlockRow(cur_transaction, plan_->GetTableOid(), table_iterator_.GetRID(), force);
  } else if (!exec_ctx_->IsDelete()) {
    switch (cur_transaction->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        break;
      case IsolationLevel::READ_COMMITTED:
        lock_manager_->UnlockRow(cur_transaction, plan_->GetTableOid(), table_iterator_.GetRID());
        break;
      case IsolationLevel::REPEATABLE_READ:
        break;
    }
  }
}

auto SeqScanExecutor::CheckIfHoldHigherLockRow(LockManager::LockMode mode, table_oid_t oid, RID rid) -> bool {
  Transaction *cur_transaction = exec_ctx_->GetTransaction();
  bool s_lock = cur_transaction->IsRowSharedLocked(oid, rid);
  bool x_lock = cur_transaction->IsRowExclusiveLocked(oid, rid);

  switch (cur_transaction->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockManager::LockMode::EXCLUSIVE) {
        return x_lock;
      }
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      switch (mode) {
        case LockManager::LockMode::SHARED:
          return s_lock || x_lock;
        case LockManager::LockMode::EXCLUSIVE:
          return x_lock;
        case LockManager::LockMode::INTENTION_SHARED:
        case LockManager::LockMode::INTENTION_EXCLUSIVE:
        case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
          throw TransactionAbortException(cur_transaction->GetTransactionId(),
                                          AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      }
      break;
  }

  return false;
}

auto SeqScanExecutor::CheckIfLockRow() -> bool {
  Transaction *cur_transaction = exec_ctx_->GetTransaction();

  bool is_lock = false;
  bool can_lock = true;
  LockManager::LockMode mode = LockManager::LockMode::SHARED;
  if (exec_ctx_->IsDelete()) {
    mode = LockManager::LockMode::EXCLUSIVE;
  }

  if (CheckIfHoldHigherLockRow(mode, plan_->GetTableOid(),
                               table_iterator_.GetRID())) {  // already hold higher level lock
    return true;
  }

  switch (cur_transaction->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockManager::LockMode::EXCLUSIVE) {
        is_lock = true;
        can_lock = lock_manager_->LockRow(cur_transaction, mode, plan_->GetTableOid(), table_iterator_.GetRID());
      }
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      is_lock = true;
      can_lock = lock_manager_->LockRow(cur_transaction, mode, plan_->GetTableOid(), table_iterator_.GetRID());
      break;
  }

  if (!can_lock) {
    throw ExecutionException("SeqScanExecutor fail to lock Tuple");
  }

  return is_lock;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iterator_.IsEnd()) {
    bool is_lock = CheckIfLockRow();  // depend on isolation level

    auto tuple_info = table_iterator_.GetTuple();
    if (!IsVisible(tuple_info.first)) {
      if (is_lock) {
        CheckIfUnlockRow(true);  // depend on isolation level
      }

      ++table_iterator_;
      continue;
    }

    if (is_lock) {
      CheckIfUnlockRow();  // depend on isolation level
    }
    tuple_info_.emplace_back(std::move(tuple_info.second), table_iterator_.GetRID());
    ++table_iterator_;
    break;
  }

  if (cnt_ == tuple_info_.size() && table_iterator_.IsEnd()) {
    return false;
  }

  *tuple = tuple_info_[cnt_].first;
  *rid = tuple_info_[cnt_].second;
  cnt_++;

  return true;
}

}  // namespace bustub
