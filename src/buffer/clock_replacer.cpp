#include "buffer/clock_replacer.h"
#include <iostream>

// 构造函数，初始化容量并将指针 hand_ 设置为 clock_list 的末尾
CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages) {}

// 析构函数，使用默认析构函数
CLOCKReplacer::~CLOCKReplacer() = default;

// 选择一个页进行替换
bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mutx_);  // 加锁以保证线程安全
  if (clock_queue.empty()) {  // 如果时钟列表为空，返回 false
    return false;
  }

  while (!clock_queue.empty()){
    frame_id_t current = clock_queue.front();  // 获取队列前面的元素
    clock_queue.pop();

    if (!clock_status[current]) {
      // 如果参考位为 0，则选择该页面进行替换
      *frame_id = current;
      clock_status.erase(current);
      return true;
    } else {
      // 如果参考位为 1，则清除参考位并将页面移动到队列后面
      clock_status[current] = false;
      clock_queue.push(current);
    }
  }
  return false;
}

// 固定一个页面，表示该页面不能被替换
void CLOCKReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutx_);  // 加锁以保证线程安全
  int size = clock_queue.size();
  for (int i = 0; i < size; i++) {
    frame_id_t current = clock_queue.front();
    clock_queue.pop();
    if (current == frame_id) {
      clock_status.erase(frame_id);
      continue;
    }
    clock_queue.push(current);
  }
}

// 取消固定一个页面，表示该页面可以被替换
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  //std::lock_guard<std::mutex> lock(mutx_);  // 加锁以保证线程安全
  frame_id_t to_delete_frame ;
  if (clock_queue.size() >= capacity) {
    std::cout << "fuck you !" << std::endl;
    this->Victim(&to_delete_frame);
    std::cout << to_delete_frame << std::endl;
    // std::cout << "fuck you !" << std::endl;
  }
  if (clock_status.find(frame_id) == clock_status.end()) {
    // 如果页面不在状态映射中，将其添加到队列和状态映射中
    clock_queue.push(frame_id);
    clock_status[frame_id] = true;
  } else {
    // 如果页面在状态映射中，将参考位设置为 1
    clock_status[frame_id] = true;
  }
}

// 返回未固定页面的数量
size_t CLOCKReplacer::Size() {
  std::lock_guard<std::mutex> lock(mutx_);  // 加锁以保证线程安全
  return clock_queue.size();  // 返回队列的大小
}