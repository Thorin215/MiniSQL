#include <cstdio>

#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
  // LOG(INFO) << "glog started!";
}

//read_state, true reps read from file,false reps read from buffer
bool read_state = false;
bool ExecFile_flag = false;
auto start_time = std::chrono::system_clock::now();
string file_name;
long file_pointer = 0;
long file_size = 99999;
void InputCommand_file(char* input, const int len){
  memset(input, 0, len);
  //initial fstraeam
  fstream file;
  file.open(file_name.c_str(),ios::in);
  //initial file_size
  file.seekg(0,ios::end);
  file_size = file.tellg();
  file_size--;
  //initial read pointer
  file.seekg(file_pointer,ios::beg);
  int i = 0;
  char ch;
  //read file
  while (file.get(ch),ch != ';') {
    if(file_pointer++>=file_size){
      read_state = false;
      file_pointer = 0;
    }
    input[i++] = ch;
  }
  input[i++] = ch;  // ;
  if(file_pointer<file_size) {
    file.get(ch);  // remove enter
  }
  file_pointer+=2;
  file.close();
}

void InputCommand(char *input, const int len) {
  memset(input, 0, len);

  printf("StarSQL> ");
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    input[i++] = ch;
  }
  input[i] = ch;  // ;
  getchar();      // remove enter
}

std::string timeToString(std::chrono::system_clock::time_point &t) {
  std::time_t time = std::chrono::system_clock::to_time_t(t);
  std::string time_str = std::ctime(&time);
  time_str.resize(time_str.size() - 1);
  return time_str;
}

int main(int argc, char **argv) {
  InitGoogleLog(argv[0]);
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];
  // executor engine
  ExecuteEngine engine;

  while (1) {
    if(read_state == true){
      //read from file
      InputCommand_file(cmd,buf_size);
    }else{
      // read from buffer
      InputCommand(cmd, buf_size);
    }
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      // Comment them out if you don't need to debug the syntax tree
      //SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      //printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
      //std::string file = "Syntax_tree_wxy.txt";
      //std::ofstream outFile(file);
      //printer.PrintTree(outFile);
      //outFile.close();
    }
    auto result = engine.Execute(MinisqlGetParserRootNode());
    //Execute in file
    if(result==DB_EXECUTE ){
      file_name = MinisqlGetParserRootNode()->child_->val_;
      start_time = std::chrono::system_clock::now();
      read_state = true;
    }
    if(file_pointer>=file_size){
      read_state = false;
      file_pointer = 0;
      auto stop_time = std::chrono::system_clock::now();
      double duration_time =
          double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
      cout << "Total time of file execution : " << fixed << setprecision(4) << duration_time / 1000 << " sec." << std::endl;
    }
    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    engine.ExecuteInformation(result);
    auto time_p = std::chrono::system_clock::now();
    cout << "Current time: " << timeToString(time_p) << endl;
    if (result == DB_QUIT) {
      break;
    }
  }
  return 0;
}