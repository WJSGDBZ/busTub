//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <climits>
#include <cstddef>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::CanTxnTakeLockTable(Transaction *txn, LockMode lock_mode) -> bool {
  AbortReason reason = AbortReason::LOCK_ON_SHRINKING;
  bool is_abort = false;
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      is_abort = true;
      reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
    } else if (txn->GetState() == TransactionState::SHRINKING) {
      is_abort = true;
      reason = AbortReason::LOCK_ON_SHRINKING;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
        lock_mode != LockMode::INTENTION_SHARED) {
      is_abort = true;
      reason = AbortReason::LOCK_ON_SHRINKING;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      is_abort = true;
      reason = AbortReason::LOCK_ON_SHRINKING;
    }
  }

  if (is_abort) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  return true;
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode curr_lock_mode, LockMode requested_lock_mode,
                                   const table_oid_t &oid, LockRequestQueue *que) -> bool {
  if (que->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  if (!CanLockUpgrade(curr_lock_mode, requested_lock_mode)) {
    return false;
  }

  UpdateTransactionTableUnLock(txn, oid, true);
  que->upgrading_ = txn->GetTransactionId();

  return true;
}

auto LockManager::UpdateTransactionTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  txn->LockTxn();
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockManager::LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockManager::LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
  txn->UnlockTxn();
  return true;
}

auto LockManager::UpdateTransactionTableUnLock(Transaction *txn, const table_oid_t &oid, bool grade) -> bool {
  txn->LockTxn();
  if (txn->IsTableSharedLocked(oid)) {
    txn->GetSharedTableLockSet()->erase(oid);
    if (!grade && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->IsTableExclusiveLocked(oid)) {
    txn->GetExclusiveTableLockSet()->erase(oid);
    if (!grade && (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED ||
                   txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
                   txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->IsTableIntentionSharedLocked(oid)) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }

  txn->UnlockTxn();
  return true;
}

void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
  for (const auto &wait_grant : lock_request_queue->request_queue_) {
    if (!wait_grant->granted_) {
      for (const auto &request : lock_request_queue->request_queue_) {
        if (request->granted_ && !AreLocksCompatible(request->lock_mode_, wait_grant->lock_mode_)) {
          return;
        }
      }

      wait_grant->granted_ = true;
    }
  }
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == requested_lock_mode) {
    return true;
  }

  switch (curr_lock_mode) {
    case LockMode::SHARED:
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
             requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
             requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE;
  }

  return false;
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE || l2 == LockMode::SHARED ||
             l2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED;
  }

  return false;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (!CanTxnTakeLockTable(txn, lock_mode)) {
    return false;
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  LockRequestQueue *que = table_lock_map_[oid].get();
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lck(que->latch_);
  // std::cout << txn->GetTransactionId() << std::endl;
  std::shared_ptr<LockRequest> request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

  bool grant = true;
  bool upgrade = false;
  for (auto request = que->request_queue_.begin(); request != que->request_queue_.end(); request++) {
    if ((*request)->txn_id_ == txn->GetTransactionId()) {  // upgrade lock
      if (!UpgradeLockTable(txn, (*request)->lock_mode_, lock_mode, oid, que)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      upgrade = true;
      que->request_queue_.erase(request);
      break;
    }
  }

  for (auto &request : que->request_queue_) {
    BUSTUB_ASSERT(request->txn_id_ != txn->GetTransactionId(), "Impossible apearance one txn twice!!");
    if (request->granted_ && !AreLocksCompatible(request->lock_mode_, lock_mode)) {
      grant = false;
    }
  }

  if (upgrade) {
    que->request_queue_.push_front(request);
  } else {
    que->request_queue_.push_back(request);
  }

  if (!grant) {
    que->cv_.wait(lck, [&]() { return request->granted_ || txn->GetState() == TransactionState::ABORTED; });
    if (txn->GetState() == TransactionState::ABORTED) {
      if (que->upgrading_ == txn->GetTransactionId()) {
        que->upgrading_ = INVALID_TXN_ID;
      }
      que->request_queue_.remove(request);
      GrantNewLocksIfPossible(que);
      que->cv_.notify_all();

      return false;
    }
  } else {
    request->granted_ = true;
  }

  // maintain transaction metadata
  if (que->upgrading_ == txn->GetTransactionId()) {
    que->upgrading_ = INVALID_TXN_ID;
  }
  UpdateTransactionTableLock(txn, lock_mode, oid);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  LockRequestQueue *que = table_lock_map_[oid].get();
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lck(que->latch_);
  if (que->request_queue_.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  size_t x_locks = 0;
  size_t s_locks = 0;
  if (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end()) {
    x_locks = txn->GetExclusiveRowLockSet()->find(oid)->second.size();
  }
  if (txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end()) {
    s_locks = txn->GetSharedRowLockSet()->find(oid)->second.size();
  }

  if (x_locks != 0 || s_locks != 0) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  bool grant = false;
  for (auto request = que->request_queue_.begin(); request != que->request_queue_.end(); request++) {
    if ((*request)->txn_id_ == txn->GetTransactionId()) {
      grant = (*request)->granted_;
      que->request_queue_.erase(request);
      break;
    }
  }

  if (grant) {
    UpdateTransactionTableUnLock(txn, oid);
    GrantNewLocksIfPossible(que);
    que->cv_.notify_all();
  }

  return true;
}

auto LockManager::CanTxnTakeLockRow(Transaction *txn, LockMode lock_mode) -> bool {
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  return CanTxnTakeLockTable(txn, lock_mode);
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockMode curr_lock_mode, LockMode requested_lock_mode,
                                 const table_oid_t &oid, const RID &rid, LockRequestQueue *que) -> bool {
  if (que->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  if (!CanLockUpgrade(curr_lock_mode, requested_lock_mode)) {
    return false;
  }

  UpdateTransactionRowUnLock(txn, oid, rid, true);
  que->upgrading_ = txn->GetTransactionId();

  return true;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  table_lock_map_latch_.lock();
  LockRequestQueue *que = table_lock_map_[oid].get();
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lck(que->latch_);
  for (auto &request : que->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (!request->granted_) {
        break;
      }
      LockMode mode = request->lock_mode_;
      switch (row_lock_mode) {
        case LockMode::SHARED:
          return mode == LockMode::INTENTION_SHARED || mode == LockMode::SHARED ||
                 mode == LockMode::INTENTION_EXCLUSIVE || mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
                 mode == LockMode::EXCLUSIVE;
        case LockMode::EXCLUSIVE:
          return mode == LockMode::INTENTION_EXCLUSIVE || mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
                 mode == LockMode::EXCLUSIVE;
        case LockMode::INTENTION_SHARED:
        case LockMode::INTENTION_EXCLUSIVE:
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          break;
      }
    }
  }

  return false;
}

auto LockManager::UpdateTransactionRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid)
    -> bool {
  txn->LockTxn();
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].insert(rid);
      break;
    case LockManager::LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }

  txn->UnlockTxn();
  return true;
}
auto LockManager::UpdateTransactionRowUnLock(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force,
                                             bool upgrade) -> bool {
  txn->LockTxn();
  if (txn->IsRowSharedLocked(oid, rid)) {
    txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);
    if (!force && !upgrade && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->IsRowExclusiveLocked(oid, rid)) {
    txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
    if (!force && !upgrade &&
        (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED ||
         txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
         txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  txn->UnlockTxn();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (!CanTxnTakeLockRow(txn, lock_mode)) {
    return false;
  }

  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  LockRequestQueue *que = row_lock_map_[rid].get();
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lck(que->latch_);
  std::shared_ptr<LockRequest> request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

  bool grant = true;
  bool upgrade = false;
  for (auto request = que->request_queue_.begin(); request != que->request_queue_.end(); request++) {
    if ((*request)->txn_id_ == txn->GetTransactionId()) {  // upgrade lock
      if (!UpgradeLockRow(txn, (*request)->lock_mode_, lock_mode, oid, rid, que)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      upgrade = true;
      que->request_queue_.erase(request);
      break;
    }
  }

  for (auto &request : que->request_queue_) {
    BUSTUB_ASSERT(request->txn_id_ != txn->GetTransactionId(), "Impossible apearance one txn twice!!");
    if (request->granted_ && !AreLocksCompatible(request->lock_mode_, lock_mode)) {
      grant = false;
    }
  }

  if (upgrade) {
    que->request_queue_.push_front(request);
  } else {
    que->request_queue_.push_back(request);
  }

  if (!grant) {
    que->cv_.wait(lck, [&]() { return request->granted_ || txn->GetState() == TransactionState::ABORTED; });
    if (txn->GetState() == TransactionState::ABORTED) {
      if (que->upgrading_ == txn->GetTransactionId()) {
        que->upgrading_ = INVALID_TXN_ID;
      }
      que->request_queue_.remove(request);
      GrantNewLocksIfPossible(que);
      que->cv_.notify_all();

      return false;
    }
  } else {
    request->granted_ = true;
  }

  // maintain transaction metadata
  if (que->upgrading_ == txn->GetTransactionId()) {
    que->upgrading_ = INVALID_TXN_ID;
  }

  UpdateTransactionRowLock(txn, lock_mode, oid, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    row_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  LockRequestQueue *que = row_lock_map_[rid].get();
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lck(que->latch_);
  if (que->request_queue_.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  bool grant = false;
  for (auto request = que->request_queue_.begin(); request != que->request_queue_.end(); request++) {
    if ((*request)->txn_id_ == txn->GetTransactionId()) {
      grant = (*request)->granted_;
      que->request_queue_.erase(request);
      break;
    }
  }

  if (grant) {
    UpdateTransactionRowUnLock(txn, oid, rid, force);
    GrantNewLocksIfPossible(que);
    que->cv_.notify_all();
  }

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

auto LockManager::FindCycle(txn_id_t source_txn, std::set<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  for (const auto &next : path) {
    if (on_path.find(next) != on_path.end()) {
      txn_id_t youngest_txn = INVALID_TXN_ID;
      for (const auto &txn_id : on_path) {
        youngest_txn = std::max(youngest_txn, txn_id);
      }

      *abort_txn_id = youngest_txn;
      return true;
    }

    on_path.insert(next);
    if (FindCycle(next, waits_for_[next], on_path, visited, abort_txn_id)) {
      return true;
    }
    on_path.erase(next);
  }

  visited.insert(source_txn);
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (t1 == t2) {
    return;
  }
  waits_for_latch_.lock();

  waits_for_[t1].insert(t2);

  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();

  waits_for_[t1].erase(t2);

  waits_for_latch_.unlock();
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_set<txn_id_t> visited;
  for (auto &start : waits_for_) {
    if (visited.find(start.first) == visited.end()) {
      std::unordered_set<txn_id_t> on_path;
      on_path.insert(start.first);
      if (FindCycle(start.first, start.second, on_path, visited, txn_id)) {
        return true;
      }
    }
  }

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  for (const auto &start : waits_for_) {
    for (const auto &end : start.second) {
      edges.emplace_back(start.first, end);
    }
  }
  return edges;
}

void LockManager::BuildWaitForGraph() {
  for (const auto &table : table_lock_map_) {
    std::vector<txn_id_t> granted;
    table.second->latch_.lock();
    for (const auto &request : table.second->request_queue_) {
      if (request->granted_) {
        granted.push_back(request->txn_id_);
      }
    }

    for (const auto &request : table.second->request_queue_) {
      if (!request->granted_) {
        for (const auto &grant : granted) {
          AddEdge(request->txn_id_, grant);
        }
      }
    }

    table.second->latch_.unlock();
  }

  for (const auto &row : row_lock_map_) {
    std::vector<txn_id_t> granted;
    row.second->latch_.lock();
    for (const auto &request : row.second->request_queue_) {
      if (request->granted_) {
        granted.push_back(request->txn_id_);
      }
    }

    for (const auto &request : row.second->request_queue_) {
      if (!request->granted_) {
        for (const auto &grant : granted) {
          AddEdge(request->txn_id_, grant);
        }
      }
    }

    row.second->latch_.unlock();
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();

      BuildWaitForGraph();

      txn_id_t be_kill_txn = INVALID_TXN_ID;
      while (HasCycle(&be_kill_txn)) {  // DFS
        // a way to notify transations that they've been aborted;
        // txn_manager_->Abort(txn_manager_->GetTransaction(be_kill_txn));
        txn_manager_->GetTransaction(be_kill_txn)->SetState(TransactionState::ABORTED);
        waits_for_[be_kill_txn].clear();
      }

      for (const auto &table : table_lock_map_) {
        table.second->cv_.notify_all();
      }
      table_lock_map_latch_.unlock();
      for (const auto &row : row_lock_map_) {
        row.second->cv_.notify_all();
      }
      row_lock_map_latch_.unlock();

      waits_for_.clear();
    }
  }
}

}  // namespace bustub
