//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), index_(nullptr), table_meta_(nullptr), table_heap_(nullptr) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  index_ = reinterpret_cast<BPlusTreeIndexType *>(index_info->index_.get());
  iter_ = index_->GetBeginIterator();
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  table_heap_ = table_meta_->table_.get();
}

void IndexScanExecutor::GetValue(Tuple* t) {
  std::vector<Value> res;
  for (auto& i : GetOutputSchema()->GetColumns()) {
    res.push_back(i.GetExpr()->Evaluate(t, &table_meta_->schema_));
  }
  *t = Tuple{res, GetOutputSchema()};
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != index_->GetEndIterator())
  {
      *rid = (*iter_).second;
      bool ok = table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
      ++iter_;

      if (ok && (plan_->GetPredicate() != nullptr || plan_->GetPredicate()->Evaluate(tuple, &table_meta_->schema_).GetAs<bool>())) {
        GetValue(tuple);
        return true;
      }
  }
  return false;
}

}  // namespace bustub
