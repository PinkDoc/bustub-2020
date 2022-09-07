//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include <utility>
#include <vector>

/**
未提交读（Read uncommitted） 指的是如果TX Abort.
他写过的数据可能被读走。
 【这不是无锁】需要在写的时候上写锁（互斥锁），
 写完放锁。读的时候不能有写锁，不能加读锁。(参考Abort Reason)

已提交读（Read committed）不会发生上述的脏读现象。
但是不可重复读：读完之后被修改，
 再读可能读到不一致的。为了解决脏读，在Commit 之前是不可以放写锁的。然后读是不能有写锁，而且要上读锁防止被写者拿走。

可重复读（Repeatable read）解决了不可重复读的问题。
这就需要2PL。Growing阶段拿锁，Shrinking阶段放锁。优化是在Shrinking阶段先放读锁，再放写锁。

upgrading字段是因为如果有两个需要升级的线程，那么可能造成死锁；在同一时刻只能由一个线程在请求升级。
 * */

namespace bustub {

void LockManager::CheckAbort(Transaction* txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    auto &[r, q] = *lock_table_.find(rid);
    for (auto iter  = q.request_queue_.begin(); iter != q.request_queue_.end(); ++iter) {
      if (iter->txn_id_ == txn->GetTransactionId()){
        q.request_queue_.erase(iter);
        // 不跳会迭代器失效
        break;
      }
    }
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::DEADLOCK};
  }
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock{latch_};
  LOG_DEBUG("LockShared");

   if (txn->GetState() == TransactionState::SHRINKING) {
     txn->SetState(TransactionState::ABORTED);
     throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
     return false;
   }


   if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
     txn->SetState(TransactionState::ABORTED);
     throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED};
     return false;
   }

   // See cppreference about piecewise-construct :)
   if (auto iter = lock_table_.find(rid); iter == lock_table_.end()) {
     lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
   }

   auto& [r, q] = *lock_table_.find(rid);
   q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
   while (txn->GetState() != TransactionState::ABORTED  && q.exclusive_count_ > 0) {
    q.cv_.wait(lock);
   }

   CheckAbort(txn, rid);

   txn->GetSharedLockSet()->emplace(rid);

   for (auto& i : q.request_queue_)
   {
     if (i.txn_id_ == txn->GetTransactionId())
     {
       i.granted_ = true;
       break;
     }
   }

   q.shared_count_++;

   return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock{latch_};
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
    return false;
  }

  if (auto iter = lock_table_.find(rid); iter == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  auto& [r, q] = *lock_table_.find(rid);

  q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  LOG_DEBUG("LockExclusive is abort %d q.exclusive %ld q.shared %ld rid %s",
            txn->GetState() == TransactionState::ABORTED, q.exclusive_count_, q.shared_count_, r.ToString().c_str());

  while (txn->GetState() != TransactionState::ABORTED  && (q.exclusive_count_ > 0 || q.shared_count_ > 0)) {
    q.cv_.wait(lock);
  }

  CheckAbort(txn, rid);

  txn->GetExclusiveLockSet()->emplace(rid);

  for (auto& i : q.request_queue_)
  {
    if (i.txn_id_ == txn->GetTransactionId())
    {
      i.granted_ = true;
      break;
    }
  }

  q.exclusive_count_++;

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock{latch_};
  LOG_DEBUG("LockUpgrade");
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
    return false;
  }

  txn->GetSharedLockSet()->erase(rid);
  auto &[r, q] = *lock_table_.find(rid);
  if (q.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT};
    return false;
  }

  q.shared_count_--;
  auto iter = q.request_queue_.begin();
  for  (; iter != q.request_queue_.end(); ++iter)
  {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  iter->lock_mode_ = LockMode::EXCLUSIVE;
  iter->granted_ = false;

  q.upgrading_ = true;
  while (txn->GetState() != TransactionState::ABORTED && (q.exclusive_count_ > 0 || q.shared_count_ > 0)) {
    q.cv_.wait(lock);
  }

  CheckAbort(txn, rid);

  for  (iter = q.request_queue_.begin() ; iter != q.request_queue_.end(); ++iter)
  {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  iter->granted_ = true;
  q.exclusive_count_++;
  q.upgrading_ = false;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  latch_.lock();
  bool is_unlock = false;
  LOG_DEBUG("Unlock %s", rid.ToString().c_str());
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto is_shared = txn->GetSharedLockSet()->erase(rid);
  auto is_exclive = txn->GetExclusiveLockSet()->erase(rid);

  auto& [r , q] = *lock_table_.find(rid);
  auto iter = q.request_queue_.begin();
  for (; iter != q.request_queue_.end(); ++iter)
  {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  q.request_queue_.erase(iter);

  if (is_shared) {
    assert(!is_exclive);
    q.shared_count_--;
    if (q.shared_count_ == 0) {
      latch_.unlock();
      is_unlock = true;
      q.cv_.notify_all();
    }
  }

  if (is_exclive) {
    assert(!is_shared);
    q.exclusive_count_--;
    latch_.unlock();
    is_unlock = true;
    q.cv_.notify_all();
  }

  if (!is_unlock) latch_.unlock();

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_[t1] = std::vector<txn_id_t>{};
    return;
  }

  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);

  if (iter == waits_for_[t1].end()) {
    return;
  }
  LOG_DEBUG("RemoveEdge {%d -> %d} Success", t1, t2);
  waits_for_[t1].erase(iter);
}

bool LockManager::Dfs(txn_id_t id) {
  bool ret = false;
  sort(waits_for_[id].begin(), waits_for_[id].end());
  for (auto i : waits_for_[id]) {
    if (visited_.find((i)) != visited_.end()) {
      return true;
    } else {
      visited_.insert(i);
      if (Dfs(i)) {
        ret = true;
        return ret;
      }
    }
  }

  return ret;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  *txn_id = -1;
  bool ret = false;
  for (auto &[i, q] : waits_for_) {
    auto has_cycle = Dfs(i);
    if (has_cycle) {
      *txn_id = std::max(*visited_.rbegin(), *txn_id);
      LOG_DEBUG("HasCycle Find Cycle %d", *txn_id);
      ret = true;
      /*
      std::cout << "HasCycle";
      for (auto j : visited_) {
        std::cout  << j << " ";
      }
      std::cout << std::endl;
       */
    }
    visited_.clear();
  }
  visited_.clear();
  return ret;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for(auto &[i, list] : waits_for_) {
    for (auto j : list)
    {
      edges.emplace_back(i, j);
    }
  }

  return edges;
}

void LockManager::RemoveCycle(txn_id_t t) {
  LOG_DEBUG("RemoveCycle %d", t);
  auto tran = TransactionManager::GetTransaction(t);
  tran->SetState(TransactionState::ABORTED);

  for (auto& rid : *tran->GetSharedLockSet()) {
    for (auto& req : lock_table_[rid].request_queue_) {
      if(!req.granted_) {
        RemoveEdge(req.txn_id_, t);
      }
    }
  }

  for (auto& rid : *tran->GetExclusiveLockSet()) {
    for (auto& req : lock_table_[rid].request_queue_) {
      if(!req.granted_) {
        RemoveEdge(req.txn_id_, t);
      }
    }
  }

  auto txn = TransactionManager::GetTransaction(t);
  txn->SetState(TransactionState::ABORTED);

  // TODO fix
  for(auto& i : lock_table_) i.second.cv_.notify_all();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here

      LOG_DEBUG("RunCycleDetection");

      // init graph
      for (auto &[r, q] : lock_table_) {
        for (auto & i : q.request_queue_) {
          if (!i.granted_) {
            for (auto& j : q.request_queue_) {
              if (j.granted_)
              {
                AddEdge(i.txn_id_, j.txn_id_);
              }
            }
          }
        }
      }

      txn_id_t remove_id = -1;
      while (HasCycle(&remove_id)) {
        RemoveCycle(remove_id);
      }

      waits_for_.clear();
      visited_.clear();
      continue;
    }
  }
}

}  // namespace bustub
