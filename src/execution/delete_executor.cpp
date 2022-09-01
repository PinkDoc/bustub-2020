//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), table_meta_(nullptr) {}

void DeleteExecutor::Init() {
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Delete(Tuple* t, RID* r) {
  bool res = table_meta_->table_->MarkDelete(*r, exec_ctx_->GetTransaction());

  for (auto& i : exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_->name_)) {
    i->index_->DeleteEntry(t->KeyFromTuple(table_meta_->schema_, i->key_schema_, i->index_->GetKeyAttrs()), *r, exec_ctx_->GetTransaction());
  }

  return res;
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (auto ok = child_executor_->Next(tuple, rid); ok) {
    return Delete(tuple, rid);
  }
  return false;
}

}  // namespace bustub
