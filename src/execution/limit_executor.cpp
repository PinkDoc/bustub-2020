//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) , plan_(plan), child_executor_(std::move(child_executor)){}

void LimitExecutor::Init() {
  child_executor_->Init();
  p_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid) && p_ <= plan_->GetOffset() + plan_->GetLimit()) {
    if (p_ < plan_->GetOffset()) {
      continue;
    }

    p_++;
    return true;
  }

  return false;
}

}  // namespace bustub
