#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"
using namespace std;
void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * wxy Implements
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  auto& req = lock_table_[rid];
  if(req.req_list_iter_map_.find(txn->GetTxnId())!=req.req_list_iter_map_.end()){
    if(req.GetLockRequestIter(txn->GetTxnId())->granted_==LockMode::kShared){
      return true;
    }
  }
  unique_lock<mutex> lock(latch_);
  if(txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted){
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(),AbortReason::kLockSharedOnReadUncommitted);
  }
  if(txn->GetState() == TxnState::kShrinking){
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(),AbortReason::kLockOnShrinking);
  }
  LockPrepare(txn,rid);
  req.EmplaceLockRequest(txn->GetTxnId(),LockMode::kShared);
  if(req.is_writing_ == true){
    req.cv_.wait(lock,[txn,&req]() -> bool{return txn->GetState()==TxnState::kAborted||!req.is_writing_;});
  }
  CheckAbort(txn,req);
  //modify txn
  txn->GetSharedLockSet().emplace(rid);
  //modify lck_manager
  req.sharing_cnt_++;
  //modify request
  req.GetLockRequestIter(txn->GetTxnId())->granted_ = LockMode::kShared;
  return true;
}

/**
 * wxy Implements
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  auto& req = lock_table_[rid];
  if(req.req_list_iter_map_.find(txn->GetTxnId())!=req.req_list_iter_map_.end()){
    if(req.GetLockRequestIter(txn->GetTxnId())->granted_==LockMode::kExclusive){
      return true;
    }
  }
  unique_lock<mutex> lock_latch(latch_);
  if(txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted){
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(),AbortReason::kLockSharedOnReadUncommitted);
  }
  if(txn->GetState() == TxnState::kShrinking){
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(),AbortReason::kLockOnShrinking);
  }
  LockPrepare(txn,rid);
  req.EmplaceLockRequest(txn->GetTxnId(),LockMode::kExclusive);
  if(req.is_writing_ == true||req.sharing_cnt_ !=0){
    req.cv_.wait(lock_latch,[txn,&req]() -> bool{
      return txn->GetState()==TxnState::kAborted||(!req.is_writing_&&req.sharing_cnt_==0);
    });
  }
  CheckAbort(txn,req);
  //modify txn
  txn->GetExclusiveLockSet().emplace(rid);
  //modify lck_manager
  req.is_writing_ = true;
  //modify request
  req.GetLockRequestIter(txn->GetTxnId())->lock_mode_ = LockMode::kExclusive;
  req.GetLockRequestIter(txn->GetTxnId())->granted_ = LockMode::kExclusive;
  return true;
}

/**
 * wxy Implements
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  auto& req = lock_table_[rid];
  if(req.req_list_iter_map_.find(txn->GetTxnId())!=req.req_list_iter_map_.end()){
    if(req.GetLockRequestIter(txn->GetTxnId())->granted_==LockMode::kExclusive){
      return true;
    }
  }
  unique_lock<mutex> lock_latch(latch_);
  if(txn->GetState() == TxnState::kShrinking){
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(),AbortReason::kLockOnShrinking);
  }

  if(req.is_upgrading_ == true){
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(),AbortReason::kUpgradeConflict);
  }
req.GetLockRequestIter(txn->GetTxnId())->lock_mode_ =LockMode::kExclusive;
  if(req.is_writing_ == true||req.sharing_cnt_ >1){
    req.is_upgrading_ = true;
    req.cv_.wait(lock_latch,[txn,&req]() -> bool{
      return txn->GetState()==TxnState::kAborted||(!req.is_writing_&&req.sharing_cnt_==1);
    });
  }
  if(txn->GetState() == TxnState::kAborted){
    req.is_upgrading_ = false;
  }
  CheckAbort(txn,req);
  //modify txn
  txn->GetSharedLockSet().erase(rid);
  txn->GetExclusiveLockSet().emplace(rid);
  //modify lck_manager
  req.is_writing_= true;
  req.is_upgrading_ = false;
  req.sharing_cnt_--;
  req.is_writing_ = true;
  //modify request
  req.GetLockRequestIter(txn->GetTxnId())->granted_ = LockMode::kExclusive;
  return true;
}

/**
 * wxy Implements
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  unique_lock<mutex> lock_latch(latch_);
  auto& req = lock_table_[rid];
  if(req.req_list_iter_map_.find(txn->GetTxnId()) == req.req_list_iter_map_.end()){
    return false;
  }
  if(txn->GetState() == TxnState::kGrowing&&txn->GetIsolationLevel() != IsolationLevel::kReadCommitted){
    txn->SetState(TxnState::kShrinking);
  }
  //modify request
  switch(req.GetLockRequestIter(txn->GetTxnId())->granted_){
    case LockMode::kShared:
      req.sharing_cnt_--;
      break;
    case LockMode::kExclusive:
      req.is_writing_ = false;
      break;
    default:
      break;
  }
  txn->GetSharedLockSet().erase(rid);
  txn->GetExclusiveLockSet().erase(rid);
  req.GetLockRequestIter(txn->GetTxnId())->granted_ = LockMode::kNone;
  req.cv_.notify_all();
  return true;
}

/**
 * wxy Implements
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
  if(txn->GetState()!= TxnState::kShrinking){
    if(lock_table_.find(rid) == lock_table_.end()){
      lock_table_.emplace(piecewise_construct, forward_as_tuple(rid), forward_as_tuple());
    }
  }else {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
}

/**
 * wxy Implements
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
  //because LockX & LockS remove the other information after 'check'
  //so we don't correct the other information
  if(txn->GetState() == TxnState::kAborted){
    req_queue.EraseLockRequest(txn->GetTxnId());
    throw TxnAbortException(txn->GetTxnId(),AbortReason::kDeadlock);
  }
}

/**
 * wxy Implements
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1] .emplace(t2);
}

/**
 * wxy Implements
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(t2);
}

/**
 * wxy Implements
 */
bool LockManager::DFS_Gwait(txn_id_t txn_id){
   if(visited_set_.find(txn_id) != visited_set_.end()){
     for(auto& item : visited_set_){
       revisited_node_ = revisited_node_>item?revisited_node_:item;
     }
     txn_id = revisited_node_;
     return true;
   }
   visited_set_.emplace(txn_id);
   visited_path_.push(txn_id);
    for(auto iter:waits_for_[txn_id]){
      if(DFS_Gwait(iter)){
        return true;
      }
    }
    visited_path_.pop();
    visited_set_.erase(txn_id);
    return false;
 }

bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  visited_set_.clear();
  stack<txn_id_t> empty_stack;
  visited_path_.swap(empty_stack);
  revisited_node_ = INVALID_TXN_ID;
  std::set<txn_id_t> all_txn;
  for(auto& iter:waits_for_){
    all_txn.emplace(iter.first);
    for(auto waited_node:iter.second){
      all_txn.emplace(waited_node);
    }
  }
  for(auto& wait_item:all_txn){
    if(DFS_Gwait(wait_item)){
      newest_tid_in_cycle = revisited_node_;
      while (!visited_path_.empty() && revisited_node_ != visited_path_.top()) {
        newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
        visited_path_.pop();
      }
      return true;
    }
  }
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id: txn->GetSharedLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
    for (const auto &row_id: txn->GetExclusiveLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
}

/**
 * wxy Implements
 */
void LockManager::RunCycleDetection() {
  while(enable_cycle_detection_==true) {
    this_thread::sleep_for(cycle_detection_interval_);
      unique_lock<mutex> lock_latch(latch_);
      waits_for_.clear();
      // construct the graph
      // find all txn
      unordered_map<RowId, set<txn_id_t>> data_txn_locked;
      unordered_map<RowId, set<txn_id_t>> data_txn_unlocked;
      for (auto& iter : lock_table_) {
        //data_id
        for (auto& request : iter.second.req_list_) {
          //txn_id
          //1.grantee locked
          if(request.granted_ != LockMode::kNone) {
            data_txn_locked[iter.first].emplace(request.txn_id_);
          }else{
            data_txn_unlocked[iter.first].emplace(request.txn_id_);
          }
        }
      }
      for (auto &iter : lock_table_) {
        //data_id
        for (auto& request : iter.second.req_list_) {
          //txn_id
          //2.addedge
          if(request.granted_ == LockMode::kNone) {
            for(auto grant:data_txn_locked[iter.first]) {
              AddEdge(request.txn_id_, grant);
            }
          }
        }
      }
      txn_id_t txn_id = INVALID_TXN_ID;
      //detect cycle
      while(HasCycle(txn_id)==true){
        DeleteNode(txn_id);
        txn_mgr_->GetTransaction(txn_id)->SetState(TxnState::kAborted);
        for(auto item:data_txn_unlocked){
          if(item.second.find(txn_id)!=item.second.end()){
            lock_table_[item.first].cv_.notify_all();
          }
        }
      }
    }
}

/**
 * wxy Implements
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;
    for(auto& iter:waits_for_){
      for(auto waited_ele : iter.second){
        result.push_back(make_pair(iter.first,waited_ele));
      }
    }
    return result;
}
