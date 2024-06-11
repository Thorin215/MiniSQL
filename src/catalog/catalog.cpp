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
  //magic_num + size * 2 + map(4, 4)*2
  return 12 +(table_meta_pages_.size() + index_meta_pages_.size()) * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  // 第一次生成数据库
  if (init) catalog_meta_ = CatalogMeta::NewInstance();
  else {
    // 读取 meta_page 的信息
    catalog_meta_ = CatalogMeta::DeserializeFrom(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    // 获取 table_meta
    for (auto iter: catalog_meta_->table_meta_pages_) {
      LoadTable(iter.first, iter.second);
    }
    // 获取 index_meta
    for (auto iter: catalog_meta_->index_meta_pages_) {
      LoadIndex(iter.first, iter.second);
    }
  }
  next_index_id_ = catalog_meta_->GetNextIndexId();
  next_table_id_ = catalog_meta_->GetNextTableId();
  FlushCatalogMetaPage();
}

CatalogManager::~CatalogManager() {
   FlushCatalogMetaPage();
   delete catalog_meta_;
   for (auto iter : tables_) {
     delete iter.second;
   }
   for (auto iter : indexes_) {
     delete iter.second;
   }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Txn *txn, TableInfo *&table_info) {
  // 已经存在同名表
  if (table_names_.count(table_name) > 0) {
    return DB_TABLE_ALREADY_EXIST;  // 返回表已存在错误
  }
  // 创建新的表
  table_info = TableInfo::Create();  // 创建表信息实例
  Schema *newschema = Schema::DeepCopySchema(schema);  // 深拷贝表模式
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, newschema, nullptr,
                                            log_manager_, lock_manager_);  // 创建表堆

  TableMetadata *table_meta = TableMetadata::Create(++next_table_id_, table_name,
                                                    table_heap->GetFirstPageId(), newschema);  // 创建表元数据
  table_info->Init(table_meta, table_heap);  // 初始化表信息

  // 序列化
  page_id_t page_id;
  Page *tablePage = buffer_pool_manager_->NewPage(page_id);  // 分配新页面
  ASSERT(page_id != INVALID_PAGE_ID && tablePage != nullptr, "unable to allocate page");  // 分配页面失败检查
  table_meta->SerializeTo(tablePage->GetData());  // 序列化表元数据到页面

  table_names_.insert(pair<string, table_id_t>(table_name, table_info->GetTableId()));  // 更新表名映射
  tables_.insert(pair<table_id_t, TableInfo *>(next_table_id_, table_info));  // 更新表信息
  catalog_meta_->table_meta_pages_.insert(pair<table_id_t, page_id_t>(next_table_id_, page_id));  // 更新表元数据页面映射

  buffer_pool_manager_->UnpinPage(page_id, true);  // 解锁页面
  buffer_pool_manager_->FlushPage(page_id);  // 刷新页面
  FlushCatalogMetaPage();  // 刷新目录元数据页面
  return DB_SUCCESS;  // 返回成功
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto itr = table_names_.find(table_name);
  if(itr == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  ASSERT(tables_.count(itr->second) > 0, "Name is found while data can't be found");
  table_info = tables_[itr->second];// 对应 id_t 存在 table
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.empty()) return DB_FAILED;
  for(auto itr = table_names_.begin(); itr != table_names_.end(); itr++) {
    tables.push_back(tables_.find(itr->second)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // 检查表是否存在
  if (table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST; // 表不存在，返回错误码
  }

  // 获取表的 ID 和信息
  table_id_t table_id = table_names_.find(table_name)->second;
  TableInfo *table_info = nullptr;
  GetTable(table_id, table_info);

  ASSERT(table_info != nullptr, "Get Table FAILED");

  // 初始化键映射
  vector<uint32_t> key_map;
  uint32_t key_index;
  table_info->GetSchema();

  // 根据索引键名获取键索引
  for (auto it = index_keys.begin(); it != index_keys.end(); it++) {
    if (table_info->GetSchema()->GetColumnIndex(*it, key_index) == DB_COLUMN_NAME_NOT_EXIST) { // 未找到在键中的列
      return DB_COLUMN_NAME_NOT_EXIST; // 返回错误码
    }
    key_map.push_back(key_index);
  }

  // 获取新的索引 ID
  next_index_id_++;

  // 检查索引是否已存在
  if (index_names_.count(table_name) > 0) {
    unordered_map<string, index_id_t> map_index_id = index_names_[table_name];
    if (map_index_id.count(index_name) > 0) {
      return DB_INDEX_ALREADY_EXIST; // 索引已存在，返回错误码
    }
    index_names_[table_name].insert(pair<string, index_id_t>(index_name, next_index_id_));
  } else {
    // 如果该表尚未存在索引，则创建一个新的索引映射
    unordered_map<string, index_id_t> map_index_Id;
    map_index_Id.insert(pair<string, index_id_t>(index_name, next_index_id_));
    index_names_.insert(pair<string, unordered_map<string, index_id_t>>(table_name, map_index_Id));
  }

  // 处理索引元数据和索引信息
  IndexMetadata *index_meta = IndexMetadata::Create(next_index_id_, index_name, table_id, key_map);
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  ASSERT(page != nullptr, "Not able to allocate new page");
  index_meta->SerializeTo(page->GetData());
  catalog_meta_->index_meta_pages_.insert(pair<index_id_t, page_id_t>(next_index_id_, page_id));

  // 初始化索引信息并插入索引集合
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.insert(pair<index_id_t, IndexInfo *>(next_index_id_, index_info));
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->FlushPage(page_id);
  FlushCatalogMetaPage();
  return DB_SUCCESS; // 创建索引成功，返回成功码
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // 检查表是否存在
  if (index_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST; // 表不存在，返回错误码
  // 检查索引是否存在
  if (index_names_.find(table_name)->second.count(index_name) == 0) return DB_INDEX_NOT_FOUND; // 索引不存在，返回错误码
  // 获取索引的 ID
  index_id_t index_id = index_names_.find(table_name)->second.find(index_name)->second;
  // 检查索引是否存在
  if (indexes_.count(index_id) == 0) return DB_FAILED; // 索引不存在，返回失败码
  // 获取索引信息并返回成功
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS; // 获取索引成功，返回成功码
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {

  // 检查表是否存在
  if (index_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST; // 表不存在，返回错误码
  //获取该表的所有索引  遍历索引映射并添加到索引向量中
  for (auto iter: index_names_.find(table_name)->second) {
    if (indexes_.count(iter.second) == 0) {
      return DB_FAILED; // 如果索引不存在，返回失败码
    }
    indexes.push_back(indexes_.find(iter.second)->second); // 添加索引到索引向量中
  }

  return DB_SUCCESS; // 获取索引成功，返回成功码
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {

  // 检查表是否存在
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST; // 表不存在，返回错误码

  // 获取表的ID和信息
  table_id_t table_id = table_names_.find(table_name)->second;
  TableInfo *table_info = tables_.find(table_id)->second;
  ASSERT(table_info != nullptr, "Table info not found"); // 确保表信息存在
  tables_.erase(table_id); // 从表信息映射中移除该表

  // 删除这个表中的所有索引
  std::vector<IndexInfo *> indexes_to_delete;
  GetTableIndexes(table_name, indexes_to_delete); // 获取表中的所有索引

  // 遍历并删除每个索引
  for (size_t i = 0; i < indexes_to_delete.size(); i++) {
    DropIndex(table_name, indexes_to_delete[i]->GetIndexName());
  }

  // 删除表的元数据页
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_.find(table_id)->second);
  catalog_meta_->table_meta_pages_.erase(table_id);

  // 注意：由于 DropIndex 使用 table_names_，因此在删除索引之后再移除这些操作
  index_names_.erase(table_name); // 从索引名称映射中移除该表
  table_names_.erase(table_name); // 从表名称映射中移除该表

  // 刷新目录元数据页
  FlushCatalogMetaPage();
  return DB_SUCCESS; // 表删除成功，返回成功码
}


/**
 * TODO: Student Implement
 */

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // 检查指定的表是否存在
  if (index_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;

  // 检查指定的索引是否存在于该表中
  if ((index_names_.find(table_name)->second).count(index_name) == 0) {
    return DB_INDEX_NOT_FOUND;
  }

  // 获取索引 ID
  index_id_t index_id = (index_names_.find(table_name)->second).find(index_name)->second;
  if (indexes_.count(index_id) == 0) return DB_FAILED;
  // 获取索引信息
  IndexInfo *index_info = indexes_.find(index_id)->second;
  // 从索引集合中移除该索引
  indexes_.erase(index_id);
  // 销毁索引（此处被注释掉）
  if(index_info->GetIndex()->Destroy() == DB_FAILED) return DB_FAILED;
  // 删除缓冲池中的页面，并从 catalog_meta_ 中移除该索引的元数据
  if(catalog_meta_->index_meta_pages_.count(index_id) == 0) return DB_FAILED;
  if(!(buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_.find(index_id)->second))) return DB_FAILED;
  catalog_meta_->index_meta_pages_.erase(index_id);

  // 从表的索引映射中移除该索引
  index_names_.find(table_name)->second.erase(index_name);

  // 刷新 catalog 元数据页
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // 将 catalog_meta_ 序列化到缓冲池管理器获取的页数据中
  catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
  // 取消页的固定，并将其标记为脏页
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  // 将页刷新到磁盘
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  TableInfo *table_info = TableInfo::Create(); // 创建TableInfo对象
  // 反序列化
  TableMetadata *table_meta = nullptr;
  char *buf = buffer_pool_manager_->FetchPage(page_id)->GetData(); // 从缓冲区中获取页数据
  ASSERT(buf != nullptr, "Buffer not get"); // 确保缓冲区不为空
  TableMetadata::DeserializeFrom(buf, table_meta); // 反序列化得到TableMetadata对象
  ASSERT(table_meta != nullptr, "Unable to deserialize table_meta_data"); // 确保TableMetadata对象不为空
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false); // 释放页

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_); // 创建TableHeap对象

  // 初始化table_info
  table_info->Init(table_meta, table_heap); // 初始化TableInfo对象
  table_names_.insert(pair<string, table_id_t>(table_info->GetTableName(), table_info->GetTableId())); // 将表名和表ID插入到table_names_中
  tables_.insert(pair<table_id_t, TableInfo *>(table_id, table_info)); // 将table_id和table_info插入到tables_中
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  IndexInfo *index_info = IndexInfo::Create(); // 创建IndexInfo对象
  // Deserialize
  IndexMetadata *index_meta = nullptr;
  char *buf = buffer_pool_manager_->FetchPage(page_id)->GetData(); // 从缓冲区中获取页数据
  ASSERT(buf != nullptr, "Buffer not get"); // 确保缓冲区不为空
  IndexMetadata::DeserializeFrom(buf, index_meta); // 反序列化得到IndexMetadata对象
  ASSERT(index_meta != nullptr, "Unable to deserialize index_meta_data"); // 确保IndexMetadata对象不为空
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false); // 释放页

  // Initialize index_info
  index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_); // 初始化IndexInfo对象
  indexes_.insert(pair<index_id_t, IndexInfo *>(index_id, index_info)); // 将index_info插入到indexes_中
  unordered_map<string, index_id_t> map_index_id;
  map_index_id.insert(pair<string, index_id_t>(index_info->GetIndexName(), index_id)); // 将索引名和索引ID插入到map_index_id中
  index_names_.insert(pair<string, unordered_map<string, index_id_t> >(index_info->GetTableInfo()->GetTableName(),
                                                                       map_index_id)); // 将表名和map_index_id插入到index_names_中
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto iter = tables_.find(table_id);
  if (iter == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = iter->second;
  return DB_SUCCESS;
}
