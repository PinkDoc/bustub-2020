//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

}

Tuple NestedLoopJoinExecutor::Join(Tuple* l, Tuple* r) {
  std::vector<Value> res;

  for (auto& i : GetOutputSchema()->GetColumns())
  {
    res.emplace_back(i.GetExpr()->EvaluateJoin(l, left_executor_->GetOutputSchema(), r, right_executor_->GetOutputSchema()));
  }

  return Tuple{res, GetOutputSchema()};
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple t1, t2;
  RID r1, r2;

  while (left_executor_->Next(&t1, &r1)) {
    if (right_executor_->Next(&t2, &r2)) {
      auto ok = plan_->Predicate()->EvaluateJoin(&t1, left_executor_->GetOutputSchema(), &t2, right_executor_->GetOutputSchema()).GetAs<bool>();
      if (ok) {
        *tuple = Join(&t1, &t2);
        return true;
      }
    }
  }

  return false;
}

}  // namespace bustub
