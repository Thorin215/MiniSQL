#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock lock{latch_};
  auto search_page = page_table_.find(page_id);
  if (search_page != page_table_.end()) {/*found*/
    frame_id_t frame_id = search_page->second;
    Page *page = &(pages_[frame_id]);
    replacer_->Pin(frame_id);
    page->pin_count_++;
    return page;
  } else {/*Unfound*/
    frame_id_t frame_id = -1;

    // Find victim page
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }

    Page *page = &(pages_[frame_id]);

    // Update page
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    page_table_.erase(page->page_id_);
    page_table_.emplace(page_id, frame_id);
    page->ResetMemory();
    page->page_id_ = page_id;

    disk_manager_->ReadPage(page_id, page->data_);
    replacer_->Pin(frame_id);
    page->pin_count_ = 1;
    return page;
  }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock{latch_};
  frame_id_t frame_id = -1;

  // Find victim page
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&frame_id)) {
    return nullptr;
  }

  page_id = AllocatePage();
  Page *page = &(pages_[frame_id]);
  page->pin_count_ = 1;

  // Update page
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  page_table_.erase(page->page_id_);
  page_table_.emplace(page_id, frame_id);
  page->ResetMemory();
  page->page_id_ = page_id;

  replacer_->Pin(frame_id);
  return page;
}

/**
 * TODO: Student Implement
 */
/*先调用disk_manager的deallocate*/
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock{latch_};
  if (page_table_.count(page_id) == 0) return true;
  frame_id_t frame_id = page_table_.find(page_id)->second;

  Page *page = &(pages_[frame_id]);
  if (page->pin_count_ > 0) return false;

  DeallocatePage(page_id);

  // Update page
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  page_table_.erase(page->page_id_);
  page_table_.emplace(INVALID_PAGE_ID, frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;

  ASSERT(page->page_id_ == INVALID_PAGE_ID, "FAILED DELETE!");
  free_list_.push_back(frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};
  // 在页表中查找页面ID
  auto search = page_table_.find(page_id);
  if (search == page_table_.end()) {
    // 如果没有找到页面，返回false
    return false;
  }
  // 获取对应的帧ID和页面对象
  frame_id_t frame_id = search->second;
  Page *page = &(pages_[frame_id]);
  // 如果页面的固定计数为0，返回false
  if (page->pin_count_ == 0) {
    return false;
  }
  // 减少页面的固定计数
  page->pin_count_--;
  // 如果固定计数减为0，将其从替换器中移除
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  // 如果页面是脏页，设置页面的脏标志
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  // 成功完成解固定操作，返回true
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::scoped_lock lock{latch_};
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto search = page_table_.find(page_id);
  if (search != page_table_.end()) {
    frame_id_t frame_id = search->second;
    Page *page = &(pages_[frame_id]);
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  } else {
    return false;
  }
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}