#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

//#include "buffer/buffer_pool_manager.h" // !!! specially added

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
    //create a Row object when constructing
    //we should implement both the rid_ and the field_
    this->curRow=new Row(rid);
    this->tableHeap=table_heap;
    if(curRow->GetRowId().Get()!=INVALID_ROWID.Get()){ //RowId != -1
        this->tableHeap->GetTuple(curRow, nullptr); //once we know rid we can deserialize and implement the row
    }
    this->txn=txn;
}

TableIterator::TableIterator(const TableIterator &other) {
    this->tableHeap=other.tableHeap;
    this->curRow=other.curRow;
    this->txn=other.txn;
}

TableIterator::~TableIterator() {

}

// !!! caution:we should notice that the we should compare RowId instead of curRow
bool TableIterator::operator==(const TableIterator &itr) const {
    return (this->curRow->GetRowId()==itr.curRow->GetRowId() && this->tableHeap==itr.tableHeap);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !(this->curRow->GetRowId()==itr.curRow->GetRowId() && this->tableHeap==itr.tableHeap);
}

const Row &TableIterator::operator*() {
    return *curRow;
}

Row *TableIterator::operator->() {
    return curRow;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    //similar to copy constructor
    this->curRow=itr.curRow;
    this->tableHeap=itr.tableHeap;
    this->txn=itr.txn;
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() { //tableHeap shall never change
    BufferPoolManager* bufferPoolManager=tableHeap->buffer_pool_manager_;
    RowId curRowId=curRow->GetRowId();
    RowId nextRowId;
    //get the pointer to current page
    TablePage* curPage=reinterpret_cast<TablePage*>
            (bufferPoolManager->FetchPage(curRowId.GetPageId()));

    //if the next row is in the same page
    if(curPage->GetNextTupleRid(curRowId,&nextRowId)){
        curRow->SetRowId(nextRowId);
        //deserialize the fields
        ASSERT(curRow->GetRowId().Get() != INVALID_ROWID.Get(),"++iter wrong");
        tableHeap->GetTuple(curRow, nullptr); //txn not used temporarily
        bufferPoolManager->UnpinPage(curPage->GetPageId(),false); //didn't modify page
        return *this;
    }
    //if the next row is not in the same page
    else{
        //ASSERT(false,"fuck");
        page_id_t nextPageId;
        do{
            nextPageId=curPage->GetNextPageId();
            if(nextPageId==INVALID_PAGE_ID){ //fail finding next available row
                curRow->SetRowId(INVALID_ROWID);
                bufferPoolManager->UnpinPage(curPage->GetPageId(),false);
                return *this;
            }
            bufferPoolManager->UnpinPage(curPage->GetPageId(),false); //finish using current page
            curPage=reinterpret_cast<TablePage*>(bufferPoolManager->FetchPage(nextPageId));
        }while(!curPage->GetFirstTupleRid(&nextRowId));

        //implement this->curRow
        curRow->SetRowId(nextRowId);
        ASSERT(curRow->GetRowId().Get() != INVALID_ROWID.Get(),"++iter wrong");
        tableHeap->GetTuple(curRow, nullptr);
        bufferPoolManager->UnpinPage(curPage->GetPageId(),false); //should be done firstly
        return *this;
    }
}

// iter++
TableIterator TableIterator::operator++(int) { //返回类型是对象
    TableIterator temp(*this);
    ++(*this);
    return TableIterator(temp);
}


