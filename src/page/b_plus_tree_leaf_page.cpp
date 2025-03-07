#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);

    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetKeySize(key_size);
    SetMaxSize(max_size);
    SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {

    int left=0;
    int right=GetSize()-1;
    int target=-1;
    while(left<=right)
    {
        int mid=(left+right)/2;
        assert(mid>=0 && mid<GetSize());
        if(KM.CompareKeys(KeyAt(mid),key)>=0){
            right=mid-1;
            target=mid;
        }else{
            left=mid+1;
        }
    }
    assert(target<GetSize());
    return target;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
    //assert(index >= 0 && index < GetSize());
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
    //assert(index >= 0 && index < GetMaxSize());
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }



/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
//!!! duplicate and lack of space not considered
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
    int pos=KeyIndex(key,KM);
    if(pos==-1){ //所有都比key小
        //insert the pair at the end
        SetKeyAt(GetSize(),key);
        SetValueAt(GetSize(),value);
        IncreaseSize(1);
        return GetSize();
    }else{
        //insert the pair right before pos
        //整体向后移动一位
        //std::cout<<pos<<std::endl;
        PairCopy(PairPtrAt(pos+1), PairPtrAt(pos),GetSize()-pos);

        SetKeyAt(pos,key);
        SetValueAt(pos,value);
        IncreaseSize(1);
        //std::cout<<GetSize()<<std::endl;
        return GetSize();
    }
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
//!!! wait and see
void LeafPage::MoveHalfTo(LeafPage *recipient) {
    ASSERT(recipient->GetSize()==0,"recipient isn't empty");
    int halfSize=GetSize()/2;
    recipient->CopyNFrom(PairPtrAt(halfSize),GetSize()-halfSize);

    //modify this
    this->SetSize(halfSize);

    //update the linked list
    recipient->SetNextPageId(this->GetNextPageId());
    this->SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
    int oldSize=GetSize();
    ASSERT(size+oldSize<=GetMaxSize(),"too large to copy");

    IncreaseSize(size);
    PairCopy(PairPtrAt(oldSize),src,size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {

    int target=KeyIndex(key,KM);
    if(target==-1 || KM.CompareKeys(KeyAt(target),key)!=0){ //not found
        return false;
    }else{
        value=ValueAt(target);
        return true;
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
    //没有参数 value，所以不好调用 LookUp
    int target=KeyIndex(key,KM);
    if(target==-1 || KM.CompareKeys(KeyAt(target),key)!=0){ //没找到要delete的record
        return GetSize();
    }else{
        //整体前移
        if(target<GetSize()-1){
            PairCopy(PairPtrAt(target), PairPtrAt(target+1),GetSize()-target-1);
        }
        IncreaseSize(-1);
        return GetSize();
    }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
/** 这里假定 MERGE 是把此节点直接合并到前一个兄弟节点中 **/
void LeafPage::MoveAllTo(LeafPage *recipient) {
    //在 recipient 的结尾插入
    recipient->CopyNFrom(this->PairPtrAt(0),this->GetSize());
    recipient->SetNextPageId(GetNextPageId());

    this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
    recipient->CopyLastFrom(this->KeyAt(0),this->ValueAt(0));
    //整体向前移一位
    PairCopy(PairPtrAt(0), PairPtrAt(1),GetSize()-1);
    SetSize(GetSize()-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
    SetKeyAt(GetSize(),key);
    SetValueAt(GetSize(),value);
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
    recipient->CopyFirstFrom(this->KeyAt(GetSize()-1),this->ValueAt(GetSize()-1));
    this->SetSize(GetSize()-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
    PairCopy(PairPtrAt(1), PairPtrAt(0),GetSize());
    SetKeyAt(0,key);
    SetValueAt(0,value);
    IncreaseSize(1);
}