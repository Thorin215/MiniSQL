#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size)
{
    if(leaf_max_size_==UNDEFINED_SIZE){
        leaf_max_size_=(PAGE_SIZE-LEAF_PAGE_HEADER_SIZE)/(KM.GetKeySize()+sizeof(RowId))-1;
    }
    if(internal_max_size_==UNDEFINED_SIZE){
        internal_max_size_=(PAGE_SIZE-LEAF_PAGE_HEADER_SIZE)/(KM.GetKeySize()+sizeof(page_id_t))-1; //!!! 有待商榷
    }

    //decide root_page_id_
    //首先拿到组织了所有索引的根节点的页面
    IndexRootsPage* indexRootsPage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    page_id_t rootPageId;
    //根据index_id_找到对应的root_id
    if(indexRootsPage->GetRootId(index_id_,&rootPageId)){
        root_page_id_=rootPageId;
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,false);
    }else{
        root_page_id_=INVALID_PAGE_ID;
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,false);
    }
}

//!!! changed destroy current page?? destroy subtree
void BPlusTree::Destroy(page_id_t current_page_id) { //recursively destroy
    //find the index_roots_page
    /*IndexRootsPage* indexRootsPage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    page_id_t rootPageId;
    if(indexRootsPage->GetRootId(index_id_, &rootPageId)){ //now wee get the root page
        BPlusTreePage* rootPage=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage())
    }else{
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,false);
    }*/
    BPlusTreePage* curPage=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(current_page_id));
    if(curPage->IsLeafPage()){ //leaf page（递归出口）
        buffer_pool_manager_->DeletePage(current_page_id);
    }else{ //internal page
        BPlusTreeInternalPage* internalPage=reinterpret_cast<BPlusTreeInternalPage*>(curPage);
        for(int index=0;index<internalPage->GetSize();index++)
        {
            Destroy(internalPage->ValueAt(index));
        }
        buffer_pool_manager_->DeletePage(current_page_id);
    }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
    return root_page_id_==INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
    if(this->IsEmpty()){
        return false;
    }
    BPlusTreeLeafPage* leafPage=reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key,INVALID_PAGE_ID, false));
    RowId res;
    if(leafPage->Lookup(key,res,processor_)){ //根据key找rid到并写入res中
        result.push_back(res);
        buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
        return true;
    }else{
        buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
        return false;
    }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
//!!! changed add duplicate check
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
    if(IsEmpty()){
        StartNewTree(key,value); //一些操作将在其中进行
        return true;
    }else{
        //先调用此类的方法来找出对应的叶结点
        /*BPlusTreeLeafPage* leafPage=reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key, INVALID_PAGE_ID, false));
        //check duplicate 在叶结点中执行
        if(leafPage->GetSize()<=leaf_max_size_-1){ //找到的leaf page空间足够
            leafPage->Insert(key,value,processor_); //!!! 这里可以insert into leaf?
            buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
            return true;
        }else{ //leaf page空间不够
            //先插入，再split
            leafPage->Insert(key,value,processor_);

        }*/
        return InsertIntoLeaf(key,value,transaction);
    }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
    //新建root_page，并为其分配page_id
    BPlusTreeLeafPage* rootPage=reinterpret_cast<BPlusTreeLeafPage*>(buffer_pool_manager_->NewPage(root_page_id_));
    if(rootPage==nullptr){
        LOG(FATAL) << "out of memory";
    }
    rootPage->Init(root_page_id_,INVALID_PAGE_ID,processor_.GetKeySize(),leaf_max_size_);
    //既是root，又是leaf
    rootPage->SetNextPageId(INVALID_PAGE_ID);
    InsertIntoLeaf(key,value,nullptr);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    //修改根表
    IndexRootsPage* indexRootsPage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    indexRootsPage->Insert(index_id_,root_page_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
//!!! changed unpin page
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
    //找到理论上要放入的leaf page
    BPlusTreeLeafPage* leafPage=reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key, INVALID_PAGE_ID, false));
    int idx=leafPage->KeyIndex(key,processor_);
    if(idx==-1 || processor_.CompareKeys(leafPage->KeyAt(idx),key)!=0){ //没找到相同的key
        if(leafPage->GetSize() <= leaf_max_size_-1){ //叶结点空间充足
            leafPage->Insert(key,value,processor_);
            buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
            return true;
        }else{ //空间不足
            //先插入后split，随后向上更新
            leafPage->Insert(key,value,processor_);
            BPlusTreeLeafPage* newLeafPage=Split(leafPage,transaction);
            GenericKey* parentKey=newLeafPage->KeyAt(0);
            ASSERT(parentKey!=nullptr,"KeyAt fail");
            InsertIntoParent(leafPage,parentKey,newLeafPage,transaction);
            buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(newLeafPage->GetPageId(), true);
            return true;
        }
    }
    else{ //duplicate
        buffer_pool_manager_->UnpinPage(leafPage->GetPageId(),false);
        return false;
    }

}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
    //在需要split的结点的右侧建立新结点
    page_id_t newPageId;
    BPlusTreeInternalPage* newPage=reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->NewPage(newPageId));
    if(newPage==nullptr){
        LOG(FATAL)<<"out of memory";
    }
    newPage->Init(newPageId,node->GetParentPageId(),processor_.GetKeySize(),internal_max_size_);
    node->MoveHalfTo(newPage,buffer_pool_manager_);
    return newPage;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
    page_id_t newPageId;
    BPlusTreeLeafPage* newPage=reinterpret_cast<BPlusTreeLeafPage*>(buffer_pool_manager_->NewPage(newPageId));
    if(newPage==nullptr){
        LOG(FATAL)<<"out of memory";
    }
    newPage->Init(newPageId,node->GetParentPageId(),processor_.GetKeySize(),leaf_max_size_);
    node->MoveHalfTo(newPage);
    return newPage;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
//!!! changed here buffer_pool_manager顺序
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
    if(old_node->IsRootPage()){ //old_node->GetParentPage==INVALID_PAGE
        page_id_t newRootPageId; //建一个新的根
        BPlusTreeInternalPage* newRootPage=reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->NewPage(newRootPageId));
        if(newRootPage== nullptr){
            LOG(FATAL)<<"out of memory";
        }
        //对新的根进行初始化
        ASSERT(newRootPage!= nullptr,"out of memory");
        this->root_page_id_=newRootPageId;
        newRootPage->Init(newRootPageId,INVALID_PAGE_ID,processor_.GetKeySize(),internal_max_size_);
        //std::cout<<internal_max_size_<<std::endl;
        newRootPage->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
        buffer_pool_manager_->UnpinPage(newRootPageId, true);
        //在IndexRootsPage中更新新的根
        IndexRootsPage* indexRootsPage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
        indexRootsPage->Update(index_id_,newRootPageId); //替换
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
        //更新子节点的父亲
        old_node->SetParentPageId(newRootPageId);
        new_node->SetParentPageId(newRootPageId);
    }
    else{ //不是根节点
        BPlusTreeInternalPage* oldParentPage=reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
        if(oldParentPage->GetSize() < oldParentPage->GetMaxSize()){ //父节点空间足够，直接插入键值对
            //std::cout<<oldParentPage->GetSize()<<std::endl;
            oldParentPage->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
            //std::cout<<oldParentPage->GetSize()<<std::endl;
            buffer_pool_manager_->UnpinPage(oldParentPage->GetPageId(), true);
        }else{ //父节点也需要分裂。，并插入父节点（向右分裂）
            //先插入，再分裂
            oldParentPage->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
            BPlusTreeInternalPage* newParentPage=Split(oldParentPage,transaction);
            GenericKey* nextParamKey=newParentPage->KeyAt(0);
            InsertIntoParent(oldParentPage,nextParamKey,newParentPage,transaction);
            newParentPage->SetKeyAt(0, nullptr);
            buffer_pool_manager_->UnpinPage(oldParentPage->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(newParentPage->GetPageId(), true);
        }
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
    if(IsEmpty()){
        return;
    }
    BPlusTreeLeafPage* leafPage= reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key,INVALID_PAGE_ID, false));
    leafPage->RemoveAndDeleteRecord(key,processor_);

    //特殊状况，删除后的pair数太少了
    if(leafPage->GetSize() < leafPage->GetMinSize() && !leafPage->IsRootPage()){
        CoalesceOrRedistribute(leafPage,transaction);
    }
    buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
    if(node->IsRootPage()){ //递归出口
        return AdjustRoot(node);
    }

    BPlusTreeInternalPage* parentPage=reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    //找到接收node的兄弟节点指针
    int nodeIndex=parentPage->ValueIndex(node->GetPageId());
    int neighborIndex;
    if(nodeIndex==0){
        neighborIndex=1;
    }else{
        neighborIndex=nodeIndex-1;
    }
    //拿取兄弟节点
    auto neighborPage=reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parentPage->ValueAt(neighborIndex)));

    //将node全部并入兄弟节点或redistribute
    if(neighborPage->GetSize()+node->GetSize() > neighborPage->GetMaxSize()){ //无法直接merge
        //redistribute完可以结束整个过程，因为parent的pair数不会改变
        Redistribute(neighborPage,node,nodeIndex); //这里已经Unpin了parentPage
        buffer_pool_manager_->UnpinPage(neighborPage->GetPageId(), true);
        return false;
    }else{
        Coalesce(neighborPage,node,parentPage,nodeIndex);
        //到这里parent的pair数已经减少了，但是parent可能也会出现pair太少的情况
        if(parentPage->GetSize() < parentPage->GetMinSize()){
            CoalesceOrRedistribute(parentPage,transaction);
        }
        return true;
    }

}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction)
{
    if(index==0){ //此时neighbor_node在右边
        //将兄弟节点并入此节点中，调整父结点指针，删除兄弟节点
        neighbor_node->MoveAllTo(node);
        parent->Remove(index+1);
        buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    }
    else{ //此时neighbor_node在左边
        node->MoveAllTo(neighbor_node);
        parent->Remove(index);
        buffer_pool_manager_->DeletePage(node->GetPageId());
    }
}


bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction)
{
    if(index==0){
        neighbor_node->MoveAllTo(node,parent->KeyAt(index+1),buffer_pool_manager_);
        parent->Remove(index+1);
        buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    }else{
        node->MoveAllTo(neighbor_node,parent->KeyAt(index),buffer_pool_manager_);
        parent->Remove(index);
        buffer_pool_manager_->DeletePage(node->GetPageId());
    }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
    BPlusTreeInternalPage* parentPage=reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    if(index==0){ //neighbor在右边
        //找到neighbor叶结点在parent中的index
        int middleKeyIndex=parentPage->ValueIndex(neighbor_node->GetPageId());
        neighbor_node->MoveFirstToEndOf(node);
        GenericKey* newMiddleKey=neighbor_node->KeyAt(0);
        parentPage->SetKeyAt(middleKeyIndex,newMiddleKey);
    }else{
        //找到node叶结点在parent中的index
        int middleKeyIndex=parentPage->ValueIndex(node->GetPageId());
        neighbor_node->MoveLastToFrontOf(node);
        GenericKey* newMiddleKey=node->KeyAt(0);
        parentPage->SetKeyAt(middleKeyIndex,newMiddleKey);
    }
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
}

//父节点key下移并更新
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
    BPlusTreeInternalPage* parentPage=reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    if(index==0){
        //找到neighbor叶结点在parent中的index
        int middleKeyIndex=parentPage->ValueIndex(neighbor_node->GetPageId());
        GenericKey* oldMiddleKey=parentPage->KeyAt(middleKeyIndex);
        GenericKey* newMiddleKey=neighbor_node->KeyAt(1);
        neighbor_node->MoveFirstToEndOf(node,oldMiddleKey,buffer_pool_manager_);
        neighbor_node->SetKeyAt(0, nullptr);
        parentPage->SetKeyAt(middleKeyIndex,newMiddleKey);
    }
    //向后移的pair的key最终会变成parent需要更新的值
    else{
        //找到node叶结点在parent中的index
        int middleKeyIndex=parentPage->ValueIndex(node->GetPageId());
        GenericKey* oldMiddleKey=parentPage->KeyAt(middleKeyIndex);
        //oldMiddleKey成为node中第一个有效的key
        GenericKey* newMiddleKey=neighbor_node->KeyAt(neighbor_node->GetSize()-1);
        neighbor_node->MoveLastToFrontOf(node,oldMiddleKey,buffer_pool_manager_);
        parentPage->SetKeyAt(middleKeyIndex,newMiddleKey);
    }
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
}



/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
    //case2:
    if(old_root_node->GetSize()==0){
        IndexRootsPage* indexRootsPage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
        indexRootsPage->Delete(this->index_id_);
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
        //此时还没有删掉root page
        return true;
    }
    //case1:one last child
    else if(old_root_node->GetSize()==1){
        root_page_id_=(reinterpret_cast<InternalPage*>(old_root_node))->RemoveAndReturnOnlyChild();
        BPlusTreeLeafPage* newRootPage=reinterpret_cast<BPlusTreeLeafPage*>(buffer_pool_manager_->FetchPage(root_page_id_));
        newRootPage->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        UpdateRootPageId();
        return true;
    }
    else{
        return false;
    }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  return IndexIterator();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
   return IndexIterator();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
//!!! changed
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
    if(IsEmpty()){
        return nullptr;
    }else{
        //start from root page
        BPlusTreePage* curPage=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_));
        while(!curPage->IsLeafPage()) //逐层向下
        {
            page_id_t nextLevelPageId;
            BPlusTreeInternalPage* internalPage=reinterpret_cast<BPlusTreeInternalPage*>(curPage);
            if(leftMost){
                nextLevelPageId=internalPage->ValueAt(0);
            }else{
                nextLevelPageId=internalPage->Lookup(key,processor_);
            }
            buffer_pool_manager_->UnpinPage(curPage->GetPageId(), false);
            curPage=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(nextLevelPageId));
        }
        //最后得到的leaf page还没有Unpin（返回之后可能要用）
        return reinterpret_cast<Page*>(curPage);
    }
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
//!!! for indexRootsPage
void BPlusTree::UpdateRootPageId(int insert_record) {
    IndexRootsPage* indexRootsPage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    if(insert_record==0){
        indexRootsPage->Update(index_id_,root_page_id_);
    }else{
        indexRootsPage->Insert(index_id_,root_page_id_);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::
ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
    std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
          //std::cout<<i<<std::endl;
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}