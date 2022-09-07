//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(std::move(child_executor)){}

void UpdateExecutor::Init() {
  child_executor_->Init();
}

bool UpdateExecutor::Update(Tuple* t, RID* r) {
  auto table = table_info_->table_.get();

  for (auto& i : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    i->index_->DeleteEntry(t->KeyFromTuple(table_info_->schema_, i->key_schema_, i->index_->GetKeyAttrs()), *r, exec_ctx_->GetTransaction());
  }

  *t = GenerateUpdatedTuple(*t);
  auto res = table->UpdateTuple(*t, *r, exec_ctx_->GetTransaction());

  if(!res) {
    // pink: delete entry ??
    return false;
  }

  for (auto& i : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    i->index_->InsertEntry(t->KeyFromTuple(table_info_->schema_, i->key_schema_, i->index_->GetKeyAttrs()), *r, exec_ctx_->GetTransaction());
  }

  return res;
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (auto ok = child_executor_->Next(tuple, rid); ok) {
    LockTuple(*rid, true);
    return Update(tuple, rid);
  }
  return false;
}
}  // namespace bustub
