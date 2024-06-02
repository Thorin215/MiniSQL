#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * wxy Implements
    */
    void Init(CheckPoint &last_checkpoint) {
      persist_lsn_ = last_checkpoint.checkpoint_lsn_;
      active_txns_ = std::move(last_checkpoint.active_txns_);
      data_ = std::move(last_checkpoint.persist_data_);
    }

    /**
    * wxy Implements
    */
    void RedoPhase() {
      for(auto log = log_recs_[persist_lsn_];
          ;log = log_recs_[++persist_lsn_]){
        //parse the log
          active_txns_[log->txn_id_] = log->lsn_;
        switch(log->type_){
          case(LogRecType::kInvalid):
            break;
          case(LogRecType::kInsert):
              data_[log->ins_key_] = log->ins_val_;
            break;
          case(LogRecType::kDelete):
              data_.erase(log->del_key_);
            break;
          case(LogRecType::kUpdate):
            data_.erase(log->old_key_);
            data_[log->new_key_]= log->new_val_;
            break;
          case(LogRecType::kBegin):
            break;
          case(LogRecType::kCommit):
              active_txns_.erase(log->txn_id_);
            break;
          case(LogRecType::kAbort):
              for(auto log_roll = log_recs_[active_txns_[log->txn_id_]];
                  ;log_roll = log_recs_[log_roll->prev_lsn_] )
              {
                switch (log_roll->type_) {
                  case(LogRecType::kInsert):
                      data_.erase(log_roll->ins_key_);
                    break;
                  case(LogRecType::kDelete):
                    data_.emplace(log_roll->del_key_,log_roll->del_val_);
                    break;
                  case (LogRecType::kUpdate):
                    data_.erase(log_roll->new_key_);
                    data_.emplace(log_roll->old_key_,log_roll->old_val_);
                    break;
                  default:
                    break;
                }
                if(log_roll->prev_lsn_ == INVALID_LSN){
                  break;
                }
              }
              active_txns_.erase(log->txn_id_);
            break;
          default:
            break;
        }
        if(persist_lsn_>=(lsn_t)(log_recs_.size()-1)){
          break;
        }
      }
    }

    /**
    * wxy Implements
    */
    void UndoPhase() {
      //abort all the active
      for(auto pair:active_txns_){
        for(auto log_roll = log_recs_[pair.second];
           ;log_roll = log_recs_[log_roll->prev_lsn_] )
        {
          if(log_roll == nullptr){
            break;
          }
          switch (log_roll->type_) {
            case(LogRecType::kInsert):
              data_.erase(log_roll->ins_key_);
              break;
            case(LogRecType::kDelete):
              data_.emplace(log_roll->del_key_,log_roll->del_val_);
              break;
            case (LogRecType::kUpdate):
              data_.erase(log_roll->new_key_);
              data_.emplace(log_roll->old_key_,log_roll->old_val_);
              break;
            default:
              break;
          }
          if(log_roll->prev_lsn_ == INVALID_LSN){
            break;
          }
        }
      }
      active_txns_.clear();
    }

    // used for test only
    void AppendLogRec(const LogRecPtr& log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
