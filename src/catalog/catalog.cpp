#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  // return 0;
  return 12 +(table_meta_pages_.size() + index_meta_pages_.size()) * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
  if(init) {
    next_table_id_ = 0;
    next_index_id_ = 0;
    catalog_meta_ = CatalogMeta::NewInstance();
  } else {

    // 反序列化，catalog meta data的逻辑页是CATALOG_META_PAGE_ID
    catalog_meta_ = CatalogMeta::DeserializeFrom(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    int count=0;
    for(auto itr = catalog_meta_->table_meta_pages_.begin(); itr != catalog_meta_->table_meta_pages_.end(); itr++) {
      count++;
    }

    // 获取table_meta
    for(auto itr = catalog_meta_->table_meta_pages_.begin(); itr != catalog_meta_->table_meta_pages_.end(); itr++) {
      LoadTable(itr->first, itr->second);
    }

    // 获取index_meta
    for(auto it = catalog_meta_->index_meta_pages_.begin(); it != catalog_meta_->index_meta_pages_.end(); it++) {
      LoadIndex(it->first, it->second);
    }

    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();
  }

  FlushCatalogMetaPage();
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  // delete catalog_meta_;
  // for (auto iter : tables_) {
  //   delete iter.second;
  // }
  // for (auto iter : indexes_) {
  //   delete iter.second;
  // }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  // return DB_FAILED;
  if(table_names_.count(table_name) > 0) {
    return DB_TABLE_ALREADY_EXIST;
  }
  table_info = TableInfo::Create();

  Schema * newschema=schema->DeepCopySchema(schema);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, newschema, nullptr,
                                            log_manager_, lock_manager_);
  //cout<<next_table_id_<<endl;
  next_table_id_++;
  int k=next_table_id_;
  //ASSERT(k== 0, "Not able to allocate page");
  //EXPECT_EQ(k,1000);
  TableMetadata *table_meta = TableMetadata::Create(next_table_id_, table_name,
                                                    table_heap->GetFirstPageId(), newschema);

  table_info->Init(table_meta, table_heap);

  // Serialize
  page_id_t page_id;
  Page *tablePage = buffer_pool_manager_->NewPage(page_id);
  ASSERT(page_id != INVALID_PAGE_ID && tablePage != nullptr, "Not able to allocate page");
  table_meta->SerializeTo(tablePage->GetData());
  /*TableMetadata *new_meta = nullptr;
  char *buf = buffer_pool_manager_->FetchPage(page_id)->GetData();
  ASSERT(buf != nullptr, "Buffer not get");
  TableMetadata::DeserializeFrom(buf, new_meta);*/
  // Insert into tables_ ...
  table_names_.insert(pair<std::string, table_id_t>(table_name, table_info->GetTableId()));
  //cout<<table_info->GetTableId()<<endl;
  tables_.insert(pair<table_id_t, TableInfo *>(next_table_id_, table_info));
  //cout<<next_table_id_<<endl;
  catalog_meta_->table_meta_pages_.insert(pair<table_id_t, page_id_t>(next_table_id_, page_id));
  //int x=page_id;
  //ASSERT(k== 0, "Not able to allocate page");
  //EXPECT_EQ(k,1000);
  buffer_pool_manager_->UnpinPage(page_id, true);
  // Write back to page
  buffer_pool_manager_->FlushPage(page_id);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto itr = table_names_.find(table_name);
  if(itr == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  ASSERT(tables_.count(itr->second) > 0, "Find table in table_names_ but not in tables_");
  table_info = tables_[itr->second];
  return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  // return DB_FAILED;
  for(auto itr = table_names_.begin(); itr != table_names_.end(); itr++) {
    tables.push_back(tables_.find(itr->second)->second);
  }
  return DB_SUCCESS;
  return DB_FAILED;
}

// dberr_t CatalogManager::GetTableNames(std::vector<string> &table_names) const {
//   for(auto itr = table_names_.begin(); itr != table_names_.end(); itr++) {
//     table_names.push_back(itr->first);
//   }
//   return DB_SUCCESS;
// }

// dberr_t CatalogManager::GetIndexNames( std::vector<string> &index_names) {
//   for(auto it:table_names_){
//     string table_name=it.first;
//     if(index_names_.count(table_name)>0){
//       for(auto it2:index_names_.find(table_name)->second) {
//         index_names.push_back(it2.first);
//       }
//     }
//   }
// }

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  //   LOG(INFO) << "here 0";
  // Get information of the table
  table_id_t table_id = table_names_.find(table_name)->second;

  TableInfo *table_info= nullptr;
  GetTable(table_id, table_info);
  //cout<<table_info->GetTableName()<<endl;
  if(table_info== nullptr)
    cout<<"false"<<endl;
  // Finding all of the keys, make the key_map
  vector<uint32_t> key_map;
  uint32_t key_index;
  //cout<<table_info->GetSchema()->GetColumns().size()<<endl;
  table_info->GetSchema();

  for(auto it = index_keys.begin(); it != index_keys.end(); it++) {
    if(table_info->GetSchema()->GetColumnIndex(*it, key_index) == DB_COLUMN_NAME_NOT_EXIST) {
      // Not found the column which is in keys

      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(key_index);
  }
  //   LOG(INFO) << "here 1";
  // Get another page of index
  next_index_id_++;
  if(index_names_.count(table_name) > 0) {
    unordered_map<string, index_id_t> map_index_id = index_names_[table_name];
    if(map_index_id.count(index_name) > 0) {
      //   LOG(INFO) << index_name;
      return DB_INDEX_ALREADY_EXIST;
    }
    index_names_[table_name].insert(pair<string, index_id_t>(index_name, next_index_id_));
  } else {
    // This table not have index yet
    // LOG(INFO) << "here 2";
    unordered_map<string, index_id_t> map_index_Id;
    map_index_Id.insert(pair<string, index_id_t>(index_name, next_index_id_));
    index_names_.insert(pair<string, unordered_map<string, index_id_t> >(table_name, map_index_Id));
  }

  // Deal with IndexInfo & indexMeta
  IndexMetadata *index_meta = IndexMetadata::Create(next_index_id_, index_name, table_id, key_map);
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  ASSERT(page != nullptr, "Not able to allocate new page");
  index_meta->SerializeTo(page->GetData());
  catalog_meta_->index_meta_pages_.insert(pair<index_id_t, page_id_t>(next_index_id_, page_id));

  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.insert(pair<index_id_t, IndexInfo *>(next_index_id_, index_info));
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->FlushPage(page_id);
  FlushCatalogMetaPage();
  //   LOG(INFO) << "here 3";
  return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  auto map_index = index_names_.find(table_name)->second;
  if(map_index.count(index_name) == 0) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = map_index.find(index_name)->second;
  if(indexes_.count(index_id) == 0) {
    return DB_FAILED;
  }
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  auto map_index_id = index_names_.find(table_name)->second;
  for(auto itr = map_index_id.begin(); itr != map_index_id.end(); itr++) {
    if(indexes_.count(itr->second) == 0) {
      return DB_FAILED;
    }
    indexes.push_back(indexes_.find(itr->second)->second);
  }
  return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  if((index_names_.find(table_name)->second).count(index_name) == 0) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = (index_names_.find(table_name)->second).find(index_name)->second;
  IndexInfo *index_info = indexes_.find(index_id)->second;
  indexes_.erase(index_id);

  //index_info->GetIndex()->Destroy();

  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_.find(index_id)->second);
  catalog_meta_->index_meta_pages_.erase(index_id);

  index_names_.find(table_name)->second.erase(index_name);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
  return DB_FAILED;
}

// dberr_t CatalogManager::DropIndex(const std::string &index_name) {
//   for(auto it:table_names_){
//     string table_name=it.first;
//   if((index_names_.find(table_name)->second).count(index_name) == 1) {

//       DropIndex(table_name,index_name);
//       return DB_SUCCESS;
//   }
//  } return DB_INDEX_NOT_FOUND;
// }

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
  return DB_FAILED;;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  TableInfo *table_info = TableInfo::Create();
  // Deserialize
  TableMetadata *table_meta = nullptr;
  char *buf = buffer_pool_manager_->FetchPage(page_id)->GetData();
  ASSERT(buf != nullptr, "Buffer not get");
  TableMetadata::DeserializeFrom(buf, table_meta);
  ASSERT(table_meta != nullptr, "Unable to deserialize table_meta_data");
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_);

  // Initialize table_info
  table_info->Init(table_meta, table_heap);
  table_names_.insert(pair<string, table_id_t>(table_info->GetTableName(), table_info->GetTableId()));
  tables_.insert(pair<table_id_t, TableInfo *>(table_id, table_info));
  return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  IndexInfo *index_info = IndexInfo::Create();
  // Deserialize
  IndexMetadata *index_meta = nullptr;
  char *buf = buffer_pool_manager_->FetchPage(page_id)->GetData();
  ASSERT(buf != nullptr, "Buffer not get");
  IndexMetadata::DeserializeFrom(buf, index_meta);
  ASSERT(index_meta != nullptr, "Unable to deserialize index_meta_data");
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  //Schema *table_schema=tables_[index_meta->GetTableId()]->GetSchema();
  //Schema *index_schema=Schema::ShallowCopySchema(table_schema, index_meta->GetKeyMapping());

  // Initialize index_info
  index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
  indexes_.insert(pair<index_id_t, IndexInfo *>(index_id, index_info));
  unordered_map<string, index_id_t> map_index_id;
  map_index_id.insert(pair<string, index_id_t>(index_info->GetIndexName(), index_id));
  index_names_.insert(pair<string,  unordered_map<string, index_id_t> >(index_info->GetTableInfo()->GetTableName(),
                                                                       map_index_id));
  return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  table_info = tables_.find(table_id)->second;
  return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_.find(table_name)->second;
  TableInfo *table_info = tables_.find(table_id)->second;
  ASSERT(table_info != nullptr, "Table info not found");
  tables_.erase(table_id);

  // 删除这个表中所有索引
  std::vector<IndexInfo *> indexes_to_delete;
  GetTableIndexes(table_name, indexes_to_delete);

  /* for(auto it = indexes_to_delete.begin(); it != indexes_to_delete.end(); it++) {
     DropIndex(table_name, it->GetIndexName());
   }*/

  for(size_t i = 0; i < indexes_to_delete.size(); i++) {
    DropIndex(table_name, indexes_to_delete[i]->GetIndexName());
  }


  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_.find(table_id)->second);
  catalog_meta_->table_meta_pages_.erase(table_id);

  // Note that DropIndex uses table_names_, there operations of erase need to be done after drop indexes
  index_names_.erase(table_name);
  table_names_.erase(table_name);

  FlushCatalogMetaPage();
  return DB_SUCCESS;
  return DB_FAILED;
}