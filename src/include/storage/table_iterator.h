#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"

//#include "table_heap.h" // !!! specially added

class TableHeap;

class TableIterator {
public:
    //constructor
    explicit TableIterator(TableHeap *table_heap, RowId rid, Txn *txn);

    //copy constructor
    explicit TableIterator(const TableIterator &other);


    virtual ~TableIterator();

    bool operator==(const TableIterator &itr) const;

    bool operator!=(const TableIterator &itr) const;

    const Row &operator*();

    Row *operator->();

    TableIterator &operator=(const TableIterator &itr) noexcept;

    TableIterator &operator++();

    TableIterator operator++(int);

private:
    // add your own private member variables here
    /* in the whole process we didn't modify the Row* pointer's pointing location */
    Row* curRow;
    TableHeap* tableHeap;
    Txn* txn;
};

#endif  // MINISQL_TABLE_ITERATOR_H