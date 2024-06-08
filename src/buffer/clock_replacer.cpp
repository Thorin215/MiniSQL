#include "buffer/clock_replacer.h"
#include <iostream>
// CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages) {
//   for (size_t i = 0; i < num_pages; i++) {
//     clock_status[i] = make_pair(true, false);
//   }
// }

// CLOCKReplacer::~CLOCKReplacer() = default;

// bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
//   frame_id_t to_replace_id = 0;
//   while (capacity > i) {
//     if (to_replace_id == clock_status.size()) {
//       to_replace_id = 0;
//     }

//     if (clock_status[to_replace_id].first) {
//       to_replace_id++;
//     } else if (clock_status[to_replace_id].second) {
//       clock_status[to_replace_id].second = false;
//       to_replace_id++;
//     } else {
//       clock_status[to_replace_id].first = true;
//       capacity--;
//       *frame_id = to_replace_id++;
//       return true;
//     }
//   }
//   return false;
// }

// void CLOCKReplacer::Pin(frame_id_t frame_id) {
//   if (clock_status.find(frame_id) != clock_status.end()) {
//     clock_status[frame_id].first = true;
//     capacity--;
//   }
// }

// void CLOCKReplacer::Unpin(frame_id_t frame_id) {
//   if (clock_status[frame_id].first) {
//     clock_status[frame_id].first = false;
//     clock_status[frame_id].second = true;
//     capacity++;
//   }
// }

// size_t CLOCKReplacer::Size() { return capacity; }

// CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages), hand_(clock_list.end()) {}

// CLOCKReplacer::~CLOCKReplacer() = default;

// bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
//   std::scoped_lock lock{mutx_};

//   if (clock_list.empty()) {
//     return false;
//   }

//   if (hand_ == clock_list.end()) {
//     hand_ = clock_list.begin();
//   }

//   while (true) {
//     if (!clock_status[*hand_].first) { // 如果未被固定
//       if (!clock_status[*hand_].second) { // 如果参考位为 0
//         *frame_id = *hand_;
//         clock_status.erase(*hand_);
//         hand_ = clock_list.erase(hand_);
//         if (hand_ == clock_list.end() && !clock_list.empty()) {
//           hand_ = clock_list.begin();
//         }
//         return true;
//       } else {
//         clock_status[*hand_].second = false;
//       }
//     }
//     ++hand_;
//     if (hand_ == clock_list.end()) {
//       hand_ = clock_list.begin();
//     }
//   }
// }

// void CLOCKReplacer::Pin(frame_id_t frame_id) {
//   std::scoped_lock lock{mutx_};
//   if (clock_status.find(frame_id) != clock_status.end()&&!clock_status[frame_id].first) {
//     clock_status[frame_id].first = true;
//     clock_list.erase(frame_id);
//   }
// }

// void CLOCKReplacer::Unpin(frame_id_t frame_id) {
//   std::scoped_lock lock{mutx_};
//   if (clock_status.find(frame_id) != clock_status.end() && !clock_status[frame_id].first) {
//     return ;
//   } else {
//     if (clock_list.size() >= capacity) {
//       clock_list.pop_front();
//     }
//     clock_list.push_back(frame_id);
//     clock_status[frame_id] = {false, true}; // 初始化为未固定且参考位为1
//   }
// }

// size_t CLOCKReplacer::Size() {return clock_list.size();}

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