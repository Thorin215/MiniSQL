#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"
#include<cstring>

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetKeySize(key_size);
    SetMaxSize(max_size);

    SetKeyAt(0, nullptr);
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) { //one page may hold many indexes and their corresponding pointers
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
    if(key== nullptr){
        memset(pairs_off+index*pair_size+key_off,0,GetKeySize());
    }else{
        memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
    }
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
    memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    //binary search
    int left=1;
    int right=GetSize()-1;
    int target=0; //default
    while(left<=right)
    {
        int mid=(left+right)/2;
        if(KM.CompareKeys(KeyAt(mid),key)<=0){
            left=mid+1;
            target=mid;
        }else{
            right=mid-1;
        }
    }
    return ValueAt(target); //可能返回最左指针
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    SetSize(2);
    SetKeyAt(0, nullptr); //key unused,pointer unknown
    SetValueAt(0,old_value);
    SetKeyAt(1,new_key);
    SetValueAt(1,new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */

//!!! changed 不能输入old_value=INVALID_PAGE_ID
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    int index= ValueIndex(old_value);
    if(index==-1){
        return -1; //no such given value
    }
    //顺延地复制index+1起往后的pairs
    if(index!=GetSize()-1){
        PairCopy(PairPtrAt(index+2), PairPtrAt(index+1),GetSize()-(index+1));
    }
    //set the new pair in its position
    SetValueAt(index+1,new_value);
    SetKeyAt(index+1,new_key);
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */

//!!! wait and see
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    //check empty
    ASSERT(recipient->GetSize()==0,"recipient not empty");

    int halfSize=GetSize()/2;
    recipient->CopyNFrom(PairPtrAt(halfSize),GetSize()-halfSize,buffer_pool_manager);
    this->SetSize(halfSize);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    int oldSize=GetSize(); //特别考虑当oldSize是0时
    ASSERT(oldSize+size<=GetMaxSize(),"the size is too large");
    PairCopy(PairPtrAt(oldSize),src,size);
    for(int i=oldSize;i<size+oldSize;i++)
    {
        page_id_t childPageId=ValueAt(i);
        //since we want to modify info in child page,we need to get the child page
        BPlusTreePage* curChildPage=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(childPageId));
        ASSERT(curChildPage != nullptr,"fail fetching");
        curChildPage->SetParentPageId(this->GetPageId());
        buffer_pool_manager->UnpinPage(childPageId,true);
    }
    SetSize(size+oldSize);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    PairCopy(PairPtrAt(index), PairPtrAt(index+1),GetSize()-index-1);
    SetSize(GetSize()-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    page_id_t val=ValueAt(0);
    SetSize(0);
    return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
/** 这里假定 MERGE 是把此节点直接合并到前一个兄弟节点中 **/
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    int thisOldSize=GetSize();
    int recipientOldSize=recipient->GetSize();
    recipient->CopyNFrom(this->PairPtrAt(0),thisOldSize,buffer_pool_manager);
    //注意index=0处的key是弃用的，所以直接复制middle_key
    recipient->SetKeyAt(recipientOldSize,middle_key);
    this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/

/** Below 2 functions are in one part **/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
//!!! 需要注意的是移动了pair之后其父节点的信息还没有修改
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
    //then update info of this
    this->Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int oldSize=this->GetSize();
    SetKeyAt(oldSize,key);
    SetValueAt(oldSize,value);
    this->IncreaseSize(1);

    BPlusTreePage* childPage=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(value));
    ASSERT(childPage!= nullptr,"fail fetching child page");
    childPage->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value,true);
}





/** Below 2 functions are in one part **/
/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    int oldSize=GetSize();
    recipient->SetKeyAt(0,middle_key);
    recipient->CopyFirstFrom(this->ValueAt(oldSize-1),buffer_pool_manager);
    //到此 pair0 is {null,value}

    this->Remove(oldSize-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int oldSize=GetSize();
    //整体向后移动一位，注意到在调用这个函数之前需要调整pair0的key（原来为null）为middle_key，这样移动之后middle_key就是首个key了
    PairCopy(PairPtrAt(1), PairPtrAt(0), oldSize);
    this->SetKeyAt(0, nullptr);
    this->SetValueAt(0, value);
    this->SetSize(oldSize+1);

    //modify child info
    BPlusTreePage* childPage=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(value));
    ASSERT(childPage!= nullptr,"fail fetching child page");
    childPage->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
}