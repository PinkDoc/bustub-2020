//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_exec_(std::move(child_executor)),  table_meta_(nullptr), idx_info_(nullptr) {}

void NestIndexJoinExecutor::Init() {
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  idx_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName() , table_meta_->name_);
}

Tuple NestIndexJoinExecutor::IndexJoin(Tuple* l, Tuple* r) {
  std::vector<Value> values;

  for (auto &i : GetOutputSchema()->GetColumns())
  {
    values.emplace_back(i.GetExpr()->EvaluateJoin(l, plan_->OuterTableSchema(), r, plan_->InnerTableSchema()));
  }
  return Tuple{values, GetOutputSchema()};
}


bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  /** Example
    SELECT column_name(s)
    FROM table1
    JOIN table2
    ON table1.column_name=table2.column_name;
 */
  Tuple left_tuple, right_tuple;

  while (true) {
    if (!rids_.empty()) {
      auto right_rid = rids_.back();
      rids_.pop_back();
      auto ok = table_meta_->table_->GetTuple(right_rid, &right_tuple, exec_ctx_->GetTransaction());
      if (!ok) {
        return false;
      }
      *tuple = IndexJoin(&left_tuple , &right_tuple);
      return ok;
    }

    if (!child_exec_->Next(&left_tuple, rid)) {
      return false;
    }

    idx_info_->index_->ScanKey(left_tuple, &rids_, exec_ctx_->GetTransaction());
  }

  return false;
}

}  // namespace bustub
