#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) { //doesn't need txn till now
    //search one by one and find a empty page
    for(page_id_t i=this->GetFirstPageId(); i!=INVALID_PAGE_ID;)
    {
        TablePage* curPage=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(i));
        if(row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW){ //tidy
            return false;
        }

        //give a write latch
        curPage->WLatch();
        //make an attempt to insert tuple in current page
        bool judge=curPage->InsertTuple(row, this->schema_, txn, this->lock_manager_, this->log_manager_);
        curPage->WUnlatch();
        if(judge){
            buffer_pool_manager_->UnpinPage(i,true);
            return true;
        }else{
            buffer_pool_manager_->UnpinPage(i,false);
            //if no existing page available,create new and update PageID
            if(curPage->GetNextPageId()==INVALID_PAGE_ID){
                //allocate a new page
                page_id_t  newPageID=INVALID_PAGE_ID;
                TablePage* newPage=reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(newPageID)); //newPageID is updated then
                newPage->Init(newPageID,i,log_manager_,txn);
                newPage->SetNextPageId(INVALID_PAGE_ID);
                //till now the new page is isolated

                curPage->SetNextPageId(newPageID);
                buffer_pool_manager_->UnpinPage(newPageID,true);

                //go to the next page,which is newly built
                i=curPage->GetNextPageId();
            }else{
                i=curPage->GetNextPageId();
            }
        }
    }
    return false;
}



bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    //firstly get the PageID from rid
    TablePage* targetPage=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    //get the old row
    Row oldRow(rid);

    //attempt to update the target row in page
    int judge=targetPage->UpdateTuple(row,&oldRow,schema_,txn,lock_manager_,log_manager_);

    bool ret;

    if(judge==1||judge==2){ //invalid slot number || already deleted
        buffer_pool_manager_->UnpinPage(targetPage->GetPageId(),false);
        ret=false;
    }else if(judge==3){ //current page doesn't have enough space
        //first delete the target tuple
        targetPage->ApplyDelete(rid,txn,log_manager_);
        buffer_pool_manager_->UnpinPage(targetPage->GetPageId(),true);
        //then insert the tuple into the heap
        if(this->InsertTuple(row, txn)){ //the rid_ of row will be updated during InsertTuple()
            ret=true;
        }else{
            ret=false;
        }
    }else if(judge==4){ //can replace directly
        //till now the target row has already been replaced
        row.SetRowId(rid);
        buffer_pool_manager_->UnpinPage(targetPage->GetPageId(),true);
        ret=true;
    }

    return ret;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  TablePage* targetPage=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));

  targetPage->WLatch();
  targetPage->ApplyDelete(rid,txn,log_manager_);
  targetPage->WUnlatch();

  //the row will be deleted after unpin page
  buffer_pool_manager_->UnpinPage(targetPage->GetTablePageId(),true);
}



void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}


/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    page_id_t targetPageId=(row->GetRowId()).GetPageId();
    //get the TablePage pointer
    TablePage* targetPage=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(targetPageId));
    bool judge=targetPage->GetTuple(row,this->schema_,txn,lock_manager_);
    buffer_pool_manager_->UnpinPage(targetPageId,false); //didn't modify
    return judge;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}


/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
    //come to the first page
    TablePage* curPage= nullptr;
    RowId curRowId=INVALID_ROWID;

    for(page_id_t i=this->GetFirstPageId();i!=INVALID_PAGE_ID;)
    {
        curPage=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(i));
        bool judge=curPage->GetFirstTupleRid(&curRowId);
        if(judge){
            buffer_pool_manager_->UnpinPage(curPage->GetPageId(),false);
            return TableIterator(this,curRowId,txn);
        }
        i=curPage->GetNextPageId();
        buffer_pool_manager_->UnpinPage(curPage->GetTablePageId(),false);
    }
    //Acquire the page read latch.
    //curPage->RLatch();
    //curPage->GetFirstTupleRid(&curRowId); //may end up invalid
    //curPage->RUnlatch();
    return TableIterator(this, curRowId, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
    return TableIterator(this, INVALID_ROWID, nullptr);
}
