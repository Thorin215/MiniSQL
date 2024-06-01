#include "buffer/clock_replacer.h"

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

CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages), hand_(clock_list.end()) {}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock{mutx_};

  if (clock_list.empty()) {
    return false;
  }

  if (hand_ == clock_list.end()) {
    hand_ = clock_list.begin();
  }

  while (true) {
    if (!clock_status[*hand_].first) { // 如果未被固定
      if (!clock_status[*hand_].second) { // 如果参考位为 0
        *frame_id = *hand_;
        clock_status.erase(*hand_);
        hand_ = clock_list.erase(hand_);
        if (hand_ == clock_list.end() && !clock_list.empty()) {
          hand_ = clock_list.begin();
        }
        return true;
      } else {
        clock_status[*hand_].second = false;
      }
    }
    ++hand_;
    if (hand_ == clock_list.end()) {
      hand_ = clock_list.begin();
    }
  }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock{mutx_};
  if (clock_status.find(frame_id) != clock_status.end()&&!clock_status[frame_id].first) {
    clock_status[frame_id].first = true;
//    clock_list.erase(frame_id);
    //wxy: For execute successfully,I have commnt it.
    //So wc need to uncommit it to merge your newest clock_replacer.cpp
    //xzkz doesn't care this file,doesn't change
  }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock{mutx_};
  if (clock_status.find(frame_id) != clock_status.end() && !clock_status[frame_id].first) {
    return ;
  } else {
    if (clock_list.size() >= capacity) {
//      clock_list.pop_front(frame_id);
      //wxy: For execute successfully,I have commnt it.
      //So wc need to uncommit it to merge your newest clock_replacer.cpp
      //xzkz doesn't care this file,doesn't change
    }
    clock_list.push_back(frame_id);
    clock_status[frame_id] = {false, true}; // 初始化为未固定且参考位为1
  }
}

size_t CLOCKReplacer::Size() {return clock_list.size();}
