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

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_oid_ = plan_->TableOid();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(table_oid_);
  table_indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  insert_finished_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (insert_finished_) {
    return false;
  }
  auto txn = GetExecutorContext()->GetTransaction();
  if (!txn->IsTableIntentionExclusiveLocked(table_oid_)) {
    auto lock_res =
        GetExecutorContext()->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid_);
    if (!lock_res) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::Exception(ExceptionType::EXECUTION, "InsertExecutor cannot get IX Lock on table");
    }
  }
  Tuple child_tuple{};
  RID r_id{};
  int64_t cnt = 0;
  auto table_heap = table_info_->table_.get();
  auto status = child_executor_->Next(&child_tuple, rid);
  auto schema = child_executor_->GetOutputSchema();
  while (status) {
    cnt += static_cast<int64_t>(table_heap->InsertTuple(child_tuple, &r_id, GetExecutorContext()->GetTransaction()));
    GetExecutorContext()->GetLockManager()->LockRow(GetExecutorContext()->GetTransaction(),
                                                    LockManager::LockMode::EXCLUSIVE, table_oid_, r_id);
    for (auto index_info : table_indexes_) {
      auto key = child_tuple.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, r_id, GetExecutorContext()->GetTransaction());
    }
    status = child_executor_->Next(&child_tuple, rid);
  }

  auto return_value = std::vector<Value>{{TypeId::BIGINT, cnt}};
  auto return_schema = Schema(std::vector<Column>{{"success_insert_count", TypeId::BIGINT}});
  *tuple = Tuple(return_value, &return_schema);
  insert_finished_ = true;
  return true;
}
}  // namespace bustub
