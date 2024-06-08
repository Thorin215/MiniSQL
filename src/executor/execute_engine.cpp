#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
   *  timestamp: 2024/6/6 19:56
   **/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_EXECUTE:
      cout << "It is file execute time Now"<<endl;
      break;
    case DB_QUIT:
      cout << "Bye. " << endl;
      cout << "©2024 By Thorin215 & Star0228 & xzkz."<< endl << "For educational use only." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(db_name.c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * wxy Implements
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  string table_name = ast->child_->val_;
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  uint32_t table_index = 0;
  for(auto table :tables){
    if(table->GetTableName()==table_name){
      return DB_TABLE_ALREADY_EXIST;
    }
  }
  vector<Column *> Table_Columns;
  vector<string > index_keys;
  pSyntaxNode column_node = ast->child_->next_->child_;
  while(column_node!= nullptr){
    if(column_node->type_ == SyntaxNodeType::kNodeColumnDefinition){
      string col_name = column_node->child_->val_;
      index_keys.push_back(col_name);
      TypeId col_type;
      Column* new_col;
      bool is_unique = (column_node->val_!= nullptr&&(string)column_node->val_=="unique")?true: false;
      if((string)column_node->child_->next_->val_=="int"){
        col_type = TypeId::kTypeInt;
      }else if((string)column_node->child_->next_->val_=="char"){
        col_type = TypeId::kTypeChar;
      }else if((string)column_node->child_->next_->val_=="float"){
        col_type = TypeId::kTypeFloat;
      }else{
        col_type = TypeId::kTypeInvalid;
      }
      if(column_node->child_->next_->child_!= nullptr){
        //char
        string check_length = column_node->child_->next_->child_->val_;
        uint32_t col_length = (uint32_t)atoi(column_node->child_->next_->child_->val_);
        if(col_type ==kTypeChar&&(check_length.find('.')!=string::npos||check_length.find('-')!=string::npos)){
          return DB_FAILED;
        }
        new_col = new Column(col_name,col_type,col_length,table_index++,true,is_unique);
      }else{
        //non-char
        new_col = new Column(col_name,col_type,table_index++, true,is_unique);
      }
      Table_Columns.push_back(new_col);

    }else if(column_node->type_ == SyntaxNodeType::kNodeColumnList){
      //primary key
      pSyntaxNode primary_node = column_node->child_;
      while(primary_node!= nullptr){
        string primary_col = primary_node->val_;
        bool is_find = false;
        for(auto& item : Table_Columns){
          if(item->GetName() == primary_col){
            item->SetTableNullable(false);
            item->SerTableUnique(true);
            is_find = true;
            break;
          }
        }
        if(is_find == false){
          return DB_KEY_NOT_FOUND;
        }
        primary_node = primary_node->next_;
      }
    }
    column_node = column_node->next_;
  }
  Schema* table_schema = new Schema(Table_Columns);
  TableInfo* tem_info ;
  auto result = dbs_[current_db_]->catalog_mgr_->CreateTable
      (table_name,table_schema->DeepCopySchema(table_schema), nullptr,tem_info);
  IndexInfo* indexInfo;
  dbs_[current_db_]->catalog_mgr_->CreateIndex(
      table_name,table_name+"_index",index_keys, nullptr,indexInfo,"bplus");
  return result;
}


/**
 * wxy Implements
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  bool flag = false;
  for(auto table :tables){
    if(table->GetTableName()==table_name){
      flag = true;
      break;
    }
  }
  if(flag== false){
    return DB_TABLE_NOT_EXIST;
  }
  dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  return DB_SUCCESS;
}

/**
 * wxy Implements nope
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  int max_w_table = 5,max_w_index=10,max_w_col=11,max_w_type=10;
  vector<TableInfo*> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  vector<IndexInfo *> Total_indexes;
  vector<IndexInfo* > indexes;
  vector<string>tem_tables_name;
  for(auto table : tables){
    max_w_table = table->GetTableName().length()>max_w_table?table->GetTableName().length():max_w_table;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(),indexes);
    for(auto index:indexes){
      tem_tables_name.push_back(table->GetTableName());
      max_w_index = index->GetIndexName().length()>max_w_index?index->GetIndexName().length():max_w_index;
      int total = -1;
      for(auto col:index->GetIndexKeySchema()->GetColumns()){
        total+=(col->GetName().length()+1);
      }
      max_w_col = total>max_w_col?total:max_w_col;
      Total_indexes.push_back(index);
    }
  }
  if(Total_indexes.empty()){
    cout<<"Empty set (0.00 sec)"<<endl;
    return DB_SUCCESS;
  }
  cout << "+" << setfill('-') << setw(max_w_table + 2) << ""
       << "+" << setfill('-') << setw(max_w_index + 2) << ""
       << "+" << setfill('-') << setw(max_w_col + 2) << ""
       << "+" << setfill('-') << setw(max_w_type + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_w_table+1) << "Table"
       << "| " << std::left << setfill(' ') << setw(max_w_index+1) << "Index_name"
       << "| " << std::left << setfill(' ') << setw(max_w_col+1) << "Column_name"
       << "| " << std::left << setfill(' ') << setw(max_w_type+1) << "Index_type"
       << "|"<<endl;

  cout << "+" << setfill('-') << setw(max_w_table + 2) << ""
       << "+" << setfill('-') << setw(max_w_index + 2) << ""
       << "+" << setfill('-') << setw(max_w_col + 2) << ""
       << "+" << setfill('-') << setw(max_w_type + 2) << ""
       << "+" << endl;
  int cnt_table = 0;
  for(auto index:Total_indexes){
    cout << "| " << std::left << setfill(' ') << setw(max_w_table+1) << tem_tables_name[cnt_table++];
    cout << "| " << std::left << setfill(' ') << setw(max_w_index+1) << index->GetIndexName();
    string tol_col;
    for(auto col:index->GetIndexKeySchema()->GetColumns()){
      if (col->GetName() == index->GetIndexKeySchema()->GetColumns()[0]->GetName()) {
        tol_col+=col->GetName();
      } else {
        tol_col += (","+col->GetName());
      }
    }
    cout << "| " << std::left << setfill(' ') << setw(max_w_col) << tol_col;
    cout << " | " << std::left << setfill(' ') << setw(max_w_type) << "BTREE"<< " |"<<endl;
  }
  cout << "+" << setfill('-') << setw(max_w_table + 2) << ""
       << "+" << setfill('-') << setw(max_w_index + 2) << ""
       << "+" << setfill('-') << setw(max_w_col + 2) << ""
       << "+" << setfill('-') << setw(max_w_type + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
  }
  return DB_SUCCESS;
}

/**
 * wxy Implements
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  string index_name = ast->child_->val_;
  string table_name_index = ast->child_->next_->val_;
  pSyntaxNode key_node = ast->child_->next_->next_->child_;
  pSyntaxNode type_node = ast->child_->next_->next_->next_;
  string index_type;
  vector<IndexInfo *> indexes;
  vector<string> index_keys;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name_index,indexes);
  for(auto item:indexes){
    if(item->GetIndexName() == index_name){
      return DB_INDEX_ALREADY_EXIST;
    }
  }
  while( key_node->type_==kNodeIdentifier){
    index_keys.push_back((string)key_node->val_);
    if(key_node->next_ == nullptr){
      break;
    }
    key_node = key_node->next_;
  }
  if(type_node!= nullptr){
    index_type = type_node->child_->val_;
  }else{
    index_type = "btree";
  }

  IndexInfo* indexInfo = nullptr;
  TableInfo* table_info;
  dbs_[current_db_]->catalog_mgr_->GetTable(table_name_index,table_info);

//  Index
  return dbs_[current_db_]->catalog_mgr_->CreateIndex(
      table_name_index,index_name,index_keys, nullptr,indexInfo,index_type);
}

/**
 * wxy Implements
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  string index_name = ast->child_->val_;
  string table_name_drop;
  vector<TableInfo*> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  vector<IndexInfo* > indexes;
  bool if_index = false;
  for(auto table : tables){
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(),indexes);
    for(auto index:indexes){
      if(index->GetIndexName() == index_name){
        if_index = true;
        table_name_drop = table->GetTableName();
        break;
      }
    }
    if(if_index == true){
      break;
    }
  }
  if(if_index == false){
    return DB_INDEX_NOT_FOUND;
  }
  dbs_[current_db_]->catalog_mgr_->DropIndex(table_name_drop,index_name);
  return DB_SUCCESS;
}


/**
 * wxy Implements
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_EXECUTE;
}

/**
 * wxy Implements
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}


//<-----Transcation --unused--->
dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}
