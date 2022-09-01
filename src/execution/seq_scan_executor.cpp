//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) :
      AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(nullptr, RID{}, nullptr),
      table_heap_(nullptr)
{}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  auto bpm = exec_ctx_->GetBufferPoolManager();
  auto first_pid = table_heap_->GetFirstPageId();
  auto page = bpm->FetchPage(first_pid);
  auto table_page = reinterpret_cast<TablePage*>(page);
  RID rid;
  table_page->GetFirstTupleRid(&rid);
  bpm->UnpinPage(first_pid, false);
  iter_ =  TableIterator{table_heap_, rid, exec_ctx_->GetTransaction()};
}

void SeqScanExecutor::GetValues(Tuple* t) {
  std::vector<Value> res;
  auto table_meta = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  for (auto& i : plan_->OutputSchema()->GetColumns())
  {
    res.push_back(i.GetExpr()->Evaluate(t, &table_meta->schema_));
  }
  *t = Tuple{res, GetOutputSchema()};
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != table_heap_->End()){
    *tuple = *iter_;
    *rid = tuple->GetRid();
    bool ok = table_heap_->GetTuple(*rid, tuple, GetExecutorContext()->GetTransaction());
    iter_++;
    // 满足条件则返回 否则继续查找
    if (ok && (plan_->GetPredicate() == nullptr ||plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>())) {
      GetValues(tuple);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
