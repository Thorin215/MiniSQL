#ifndef MINISQL_CLOCK_REPLACER_H
#define MINISQL_CLOCK_REPLACER_H

#include <algorithm>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * CLOCKReplacer implements the clock replacement.
 */
class CLOCKReplacer : public Replacer {
 public:
  /**
   * Create a new CLOCKReplacer.
   * @param num_pages the maximum number of pages the CLOCKReplacer will be required to store
   */
  explicit CLOCKReplacer(size_t num_pages);

  /**
   * Destroys the CLOCKReplacer.
   */
  ~CLOCKReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // std::mutex mutx_;                                    // 线程锁
  // size_t capacity;                                     // 容量
  // std::list<frame_id_t> clock_list;                    // 可以被替换的页列表
  // std::unordered_map<frame_id_t, std::pair<bool, bool>> clock_status; // 页状态: <是否固定, 时钟参考位>
  // std::list<frame_id_t>::iterator hand_;               // 当前指针

    std::mutex mutx_;
    size_t capacity;
    std::queue<frame_id_t> clock_queue;
    std::unordered_map<frame_id_t, bool> clock_status; // 页状态: 参考位 (bitref)
};

#endif  // MINISQL_CLOCK_REPLACER_H