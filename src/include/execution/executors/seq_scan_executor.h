//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  auto IsVisible(TupleMeta &tuple_meta) -> bool;
  auto CheckIfLockTable() -> bool;
  auto CheckIfHoldHigherLockTable(LockManager::LockMode mode, table_oid_t oid) -> bool;
  auto CheckIfLockRow() -> bool;
  auto CheckIfHoldHigherLockRow(LockManager::LockMode mode, table_oid_t oid, RID rid) -> bool;
  void CheckIfUnlockRow(bool force = false);
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  const TableInfo *table_info_;
  TableIterator table_iterator_;
  LockManager *lock_manager_;
  TransactionManager *transaction_manager_;

  std::vector<std::pair<Tuple, RID>> tuple_info_;
  size_t cnt_{0};
  bool done_{false};
};
}  // namespace bustub
