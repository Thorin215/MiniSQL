#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * wxy Implements
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};
    KeyType ins_key_,del_key_,old_key_,new_key_;
    [[maybe_unused]] ValType ins_val_{},del_val_{},old_val_{},new_val_{};

    LogRec(LogRecType type,lsn_t lsn,lsn_t prev_lsn,txn_id_t txn_id):
    type_(type),lsn_(lsn),prev_lsn_(prev_lsn),txn_id_(txn_id)
    {}

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * wxy Implements
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  lsn_t prev_lsn;
  if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
    prev_lsn = INVALID_LSN ;
    LogRec::prev_lsn_map_.emplace(txn_id,LogRec::next_lsn_);
  }else{
    prev_lsn = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }
  LogRec log = LogRec(LogRecType::kInsert,LogRec::next_lsn_++,prev_lsn,txn_id);
  log.ins_key_ = std::move(ins_key);
  log.ins_val_ = ins_val;
  return std::make_shared<LogRec>(log);
}

/**
 * wxy Implements
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  lsn_t prev_lsn;
  if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
    prev_lsn = INVALID_LSN ;
    LogRec::prev_lsn_map_.emplace(txn_id,LogRec::next_lsn_);
  }else{
    prev_lsn = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }
  LogRec log = LogRec(LogRecType::kDelete,LogRec::next_lsn_++,prev_lsn,txn_id);
  log.del_key_ = std::move(del_key);
  log.del_val_ = del_val;
  return std::make_shared<LogRec>(log);
}

/**
 * wxy Implements
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  lsn_t prev_lsn;
  if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
    prev_lsn = INVALID_LSN ;
    LogRec::prev_lsn_map_.emplace(txn_id,LogRec::next_lsn_);
  }else{
    prev_lsn = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }
  LogRec log = LogRec(LogRecType::kUpdate,LogRec::next_lsn_++,prev_lsn,txn_id);
  log.old_key_ = std::move(old_key);
  log.new_key_ = std::move(new_key);
  log.new_val_ = new_val;
  log.old_val_ = old_val;
  return std::make_shared<LogRec>(log);
}

/**
 * wxy Implements
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id){
  lsn_t prev_lsn;
  if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
    prev_lsn = INVALID_LSN ;
    LogRec::prev_lsn_map_.emplace(txn_id,LogRec::next_lsn_);
  }else{
    prev_lsn = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }
  LogRec log = LogRec(LogRecType::kBegin,LogRec::next_lsn_++,prev_lsn,txn_id);
  return std::make_shared<LogRec>(log);
}

/**
 * wxy Implements
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id){
  lsn_t prev_lsn;
  if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
    prev_lsn = INVALID_LSN ;
    LogRec::prev_lsn_map_.emplace(txn_id,LogRec::next_lsn_);
  }else{
    prev_lsn = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }
  LogRec log = LogRec(LogRecType::kCommit,LogRec::next_lsn_++,prev_lsn,txn_id);
  return std::make_shared<LogRec>(log);
}

/**
 * wxy Implements
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  lsn_t prev_lsn;
  if(LogRec::prev_lsn_map_.find(txn_id)==LogRec::prev_lsn_map_.end()) {
    prev_lsn = INVALID_LSN ;
    LogRec::prev_lsn_map_.emplace(txn_id,LogRec::next_lsn_);
  }else{
    prev_lsn = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
  }
  LogRec log = LogRec(LogRecType::kAbort,LogRec::next_lsn_++,prev_lsn,txn_id);
  return std::make_shared<LogRec>(log);
}

#endif  // MINISQL_LOG_REC_H
