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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  table_oid_ = plan_->GetTableOid();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(table_oid_);
  table_heap_ = table_info_->table_.get();
}

void SeqScanExecutor::Init() { table_iterator_ = table_heap_->Begin(GetExecutorContext()->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (table_iterator_ != table_heap_->End()) {
    *tuple = *table_iterator_;
    *rid = tuple->GetRid();

    bool res = table_heap_->GetTuple(*rid, tuple, GetExecutorContext()->GetTransaction());
    ++table_iterator_;
    if (!res) {
      continue;
    }
    if (plan_->filter_predicate_ == nullptr ||
        plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      return true;
    }
  }
  return false;
}
}  // namespace bustub
