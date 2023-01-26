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
#include <sys/types.h>
#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

static const uint8_t LOCK_COMPATIBLE_MATRIX[5][5] = {
    {1, 1, 1, 1, 0}, {1, 1, 0, 0, 0}, {1, 0, 1, 0, 0}, {1, 0, 0, 0, 0}, {0, 0, 0, 0, 0},
};
static const uint8_t LOCK_UPGRADE_COMPATIBLE_MATRIX[5][5]{
    {0, 0, 1, 1, 1}, {0, 0, 0, 1, 1}, {0, 0, 0, 1, 1}, {0, 0, 0, 0, 0}, {0, 0, 0, 0, 0},
};

auto LockManager::TryAcquireLock(Transaction *txn, const std::shared_ptr<LockRequestQueue> &queue, LockMode lock_mode)
    -> bool {
  // check compatible
  for (const auto &req : queue->request_queue_) {
    if (req->granted_ && !CheckCompatible(req->lock_mode_, lock_mode) && req->txn_id_ != txn->GetTransactionId()) {
      return false;
    }
  }
  // check priority
  // upgrading
  if (queue->upgrading_ != INVALID_TXN_ID && queue->upgrading_ != txn->GetTransactionId()) {
    return false;
  }

  // find first waiting request
  auto it = queue->request_queue_.begin();
  std::list<std::shared_ptr<LockRequest>>::iterator first_waiting;
  for (; it != queue->request_queue_.end(); it++) {
    if (!(*it)->granted_) {
      first_waiting = it;
      break;
    }
  }

  if (!(*it)->granted_ && (*it)->txn_id_ == txn->GetTransactionId()) {
    return true;
  }

  bool compatible = true;
  LockMode prv_lock = (*it)->lock_mode_;
  std::advance(it, 1);
  for (; it != queue->request_queue_.end(); it++) {
    compatible = compatible && CheckCompatible(prv_lock, (*it)->lock_mode_);
    prv_lock = (*it)->lock_mode_;
  }
  return compatible;
}

auto LockManager::CheckCompatible(LockMode hold_lock_mode, LockMode want_lock_mode) -> bool {
  return LOCK_COMPATIBLE_MATRIX[static_cast<int>(hold_lock_mode)][static_cast<int>(want_lock_mode)] == 1U;
}

// *        IS -> [S, X, SIX]
// *        S -> [X, SIX]
// *        IX -> [X, SIX]
// *        SIX -> [X]

auto LockManager::CheckUpgradingCompatible(LockMode old_lock_mode, LockMode new_lock_mode) -> bool {
  return LOCK_UPGRADE_COMPATIBLE_MATRIX[static_cast<int>(old_lock_mode)][static_cast<int>(new_lock_mode)] == 1U;
}

void LockManager::TxnInsertTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
  }
}

void LockManager::TxnRemoveTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
  }
}

void LockManager::TxnInsertRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    if (txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
      txn->GetSharedRowLockSet()->emplace(oid, std::unordered_set<RID>());
    }
    txn->GetSharedRowLockSet()->find(oid)->second.insert(rid);
    txn->GetSharedLockSet()->insert(rid);
  } else {
    if (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetExclusiveRowLockSet()->end()) {
      txn->GetExclusiveRowLockSet()->emplace(oid, std::unordered_set<RID>());
    }
    txn->GetExclusiveRowLockSet()->find(oid)->second.insert(rid);
    txn->GetExclusiveLockSet()->insert(rid);
  }
}

void LockManager::TxnRemoveRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->erase(rid);
    txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);
  } else {
    txn->GetExclusiveLockSet()->erase(rid);
    txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // try {
    /*=================== check txn state ========================*/
    if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
      throw Exception("Impossible Error!!!");
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        case IsolationLevel::READ_UNCOMMITTED:
          if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
          } else {
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
          }
        case IsolationLevel::READ_COMMITTED:
          if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
          }
      }
    }
    if (txn->GetState() == TransactionState::GROWING) {
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
          (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
    /*============================================================*/
    auto thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 100;
    table_lock_map_latch_.lock();
    LOG_DEBUG("Thread %zu Try lock table %d", thread_id, oid);
    // create new lock request.
    auto lock_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    // lock the request queue
    auto has_que = table_lock_map_.find(oid);
    // create new lock requeset queue
    if (has_que == table_lock_map_.end()) {
      auto new_req_que = std::make_shared<LockRequestQueue>();
      lock_req->granted_ = true;
      new_req_que->request_queue_.emplace_back(lock_req);
      table_lock_map_.emplace(oid, new_req_que);
      TxnInsertTableLock(txn, lock_mode, oid);
      table_lock_map_latch_.unlock();
      return true;
    }

    auto lock_req_que = has_que->second;
    std::unique_lock<std::mutex> queue_lock(lock_req_que->latch_);
    // release the map latch
    table_lock_map_latch_.unlock();

    /*================== check lock upgrading ====================*/
    auto it = std::find_if(lock_req_que->request_queue_.begin(), lock_req_que->request_queue_.end(),
                           [&](const auto &req) { return req->txn_id_ == txn->GetTransactionId(); });
    if (it != lock_req_que->request_queue_.end()) {
      // check
      assert(lock_req_que->upgrading_ == INVALID_TXN_ID);
      auto old_lock_mode = (*it)->lock_mode_;
      if (old_lock_mode == lock_mode) {
        return true;
      }
      LOG_DEBUG("Start Upgrading");
      if (lock_req_que->upgrading_ != INVALID_PAGE_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!CheckUpgradingCompatible(old_lock_mode, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // upgrading
      lock_req_que->request_queue_.erase(it);
      TxnRemoveTableLock(txn, old_lock_mode, oid);
      lock_req_que->upgrading_ = txn->GetTransactionId();
    }
    /*============================================================*/

    // add the request into the request queue
    lock_req_que->request_queue_.emplace_back(lock_req);
    /*================== try to acquire the lock =================*/
    while (!TryAcquireLock(txn, lock_req_que, lock_mode)) {
      LOG_DEBUG("sleep");
      lock_req_que->cv_.wait(queue_lock);
    }
    lock_req->granted_ = true;
    if (lock_req_que->upgrading_ == txn->GetTransactionId()) {
      lock_req_que->upgrading_ = INVALID_TXN_ID;
    }
  // } catch (TransactionAbortException e) {
    // LOG_DEBUG("%s", e.GetInfo().c_str());
  // }

  // booking keeping
  TxnInsertTableLock(txn, lock_mode, oid);
  return true;
  /*===========================================================*/
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  auto thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 100;

  // check row lock
  if (!(txn->GetSharedLockSet()->empty() && txn->GetExclusiveLockSet()->empty())) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  LOG_DEBUG("Thread %zu Try unlock table %d", thread_id, oid);
  auto lock_req_que = table_lock_map_.find(oid)->second;
  std::unique_lock<std::mutex> queue_lock(lock_req_que->latch_);
  // release the map latch
  table_lock_map_latch_.unlock();

  std::shared_ptr<LockRequest> request = nullptr;
  for (const auto &req : lock_req_que->request_queue_) {
    if (req->granted_ && req->txn_id_ == txn->GetTransactionId()) {
      request = req;
      break;
    }
  }
  if (request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if (txn->GetState() == TransactionState::GROWING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
      case IsolationLevel::READ_COMMITTED:
        if (request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
  // booking keeping
  TxnRemoveTableLock(txn, request->lock_mode_, oid);
  lock_req_que->request_queue_.remove(request);
  LOG_DEBUG("Thread %zu notify all", thread_id);
  lock_req_que->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  /*=================== check table lock =======================*/
  // try {
    // bool is_table_lock = true;
    // if (lock_mode == LockMode::SHARED) {
    bool is_table_lock = txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
                         txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
                         txn->IsTableIntentionExclusiveLocked(oid);
    // } else {
    // is_table_lock = txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
    // txn->IsTableSharedIntentionExclusiveLocked(oid);
    // }
    if (!is_table_lock) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    /*============================================================*/

    /*=================== check txn state ========================*/
    if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
      throw Exception("Impossible Error!!!");
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        case IsolationLevel::READ_UNCOMMITTED:
          if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
          } else {
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
          }
        case IsolationLevel::READ_COMMITTED:
          if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
          }
      }
    }
    if (txn->GetState() == TransactionState::GROWING) {
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
          (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
    /*============================================================*/

    row_lock_map_latch_.lock();
    // create new lock request.
    auto lock_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    // lock the request queue
    auto has_que = row_lock_map_.find(rid);
    // create new lock requeset queue
    if (has_que == row_lock_map_.end()) {
      auto new_req_que = std::make_shared<LockRequestQueue>();
      lock_req->granted_ = true;
      new_req_que->request_queue_.emplace_back(lock_req);
      row_lock_map_.emplace(rid, new_req_que);
      TxnInsertRowLock(txn, lock_mode, oid, rid);
      row_lock_map_latch_.unlock();
      return true;
    }

    auto lock_req_que = has_que->second;
    std::unique_lock<std::mutex> queue_lock(lock_req_que->latch_);
    // release the map latch
    row_lock_map_latch_.unlock();

    /*================== check lock upgrading ====================*/
    auto it = std::find_if(lock_req_que->request_queue_.begin(), lock_req_que->request_queue_.end(),
                           [&](const auto &req) { return req->txn_id_ == txn->GetTransactionId(); });
    if (it != lock_req_que->request_queue_.end()) {
      // check
      auto old_lock_mode = (*it)->lock_mode_;
      assert(lock_req_que->upgrading_ == INVALID_TXN_ID);
      if (old_lock_mode == lock_mode) {
        return true;
      }
      if (lock_req_que->upgrading_ != INVALID_PAGE_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!CheckUpgradingCompatible(old_lock_mode, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // upgrading
      lock_req_que->request_queue_.erase(it);
      TxnRemoveRowLock(txn, old_lock_mode, oid, rid);
      lock_req_que->upgrading_ = txn->GetTransactionId();
    }
    /*============================================================*/

    // add the request into the request queue
    lock_req_que->request_queue_.emplace_back(lock_req);
    /*================== try to acquire the lock =================*/
    while (!TryAcquireLock(txn, lock_req_que, lock_mode)) {
      lock_req_que->cv_.wait(queue_lock);
    }
    lock_req->granted_ = true;
    if (lock_req_que->upgrading_ == txn->GetTransactionId()) {
      lock_req_que->upgrading_ = INVALID_TXN_ID;
    }
    /*===========================================================*/
  // } catch (TransactionAbortException e) {
    // LOG_DEBUG("%s", e.GetInfo().c_str());
  // }

  // booking keeping
  TxnInsertRowLock(txn, lock_mode, oid, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  auto lock_req_que = row_lock_map_.find(rid)->second;
  std::lock_guard<std::mutex> queue_lock(lock_req_que->latch_);
  // release the map latch
  row_lock_map_latch_.unlock();

  std::shared_ptr<LockRequest> request = nullptr;
  for (const auto &req : lock_req_que->request_queue_) {
    if (req->granted_ && req->txn_id_ == txn->GetTransactionId()) {
      request = req;
      break;
    }
  }
  if (request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if (txn->GetState() == TransactionState::GROWING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
      case IsolationLevel::READ_COMMITTED:
        if (request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
  // booking keeping
  TxnRemoveRowLock(txn, request->lock_mode_, oid, rid);
  lock_req_que->request_queue_.remove(request);
  lock_req_que->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
