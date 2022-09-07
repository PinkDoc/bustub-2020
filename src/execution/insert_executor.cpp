//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

/**
 * 两种插入方法 1. 从child_exec 中插入 2. 从plan中插入
 * */

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)), table_meta_(nullptr), table_(nullptr), catalog_(nullptr) {}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_meta_ = catalog_->GetTable(plan_->TableOid());
  table_ = table_meta_->table_.get();

  if (!plan_->IsRawInsert()) {
    child_exec_->Init();
  }

  if (plan_->IsRawInsert()) {
    iter_ = plan_->RawValues().begin();
  }
}

bool InsertExecutor::Insert(Tuple* t, RID* r) {
  auto res = table_->InsertTuple(*t, r, exec_ctx_->GetTransaction());

  LockTuple(*r, true);

  if (!res) return false;

  for (auto& i : catalog_->GetTableIndexes(table_meta_->name_)) {
    i->index_->InsertEntry(t->KeyFromTuple(table_meta_->schema_, i->key_schema_, i->index_->GetKeyAttrs()), *r , exec_ctx_->GetTransaction());
  }

  return true;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {

  if (!plan_->IsRawInsert()) {
    if (auto ok = child_exec_->Next(tuple, rid); ok) {
      return Insert(tuple, rid);
    }
    return false;
  } else {
    if (iter_ != plan_->RawValues().end()) {
      auto t = Tuple{*iter_++, &table_meta_->schema_};
      return Insert(&t, rid);
    }

    return false;
  }
  return false;

}

}  // namespace bustub
