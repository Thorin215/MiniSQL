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

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
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
        if (!find_victim_page(&frame_id)) {
            return nullptr;
        }/*new place to fetch new page*/
        Page *page = &(pages_[frame_id]);
        update_page(page, page_id, frame_id);
        disk_manager_->ReadPage(page_id, page->data_);
        replacer_->Pin(frame_id);
        page->pin_count_ = 1;
        return page;
    }
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    std::scoped_lock lock{latch_};
    frame_id_t frame_id = -1;
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }
    page_id = AllocatePage();
    Page *page = &(pages_[frame_id]);
    page->pin_count_ = 1;
    update_page(page, page_id, frame_id);
    replacer_->Pin(frame_id);
    return page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::scoped_lock lock{latch_};
    auto search = page_table_.find(page_id);
    if (search == page_table_.end()) {
        return true;
    }
    frame_id_t frame_id = search->second;
    Page *page = &(pages_[frame_id]);
    if (page->pin_count_ > 0) {
        return false;
    }
    DeallocatePage(page_id);
    update_page(page, INVALID_PAGE_ID, frame_id);
    free_list_.push_back(frame_id);
    return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::scoped_lock lock{latch_};
    auto search = page_table_.find(page_id);
    if (search == page_table_.end()) {
        return false;
    }
    frame_id_t frame_id = search->second;
    Page *page = &(pages_[frame_id]);
    if (page->pin_count_ == 0) {
        return false;
    }
    page->pin_count_--;
    if (page->pin_count_ == 0) {
        replacer_->Unpin(frame_id);
    }
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    return true;
}

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

void BufferPoolManager::FlushAllPages() {
    std::scoped_lock lock{latch_};
    for (size_t i = 0; i < pool_size_; i++) {
        Page *page = &(pages_[i]);
        if (page->page_id_ != INVALID_PAGE_ID && page->IsDirty()) {
            disk_manager_->WritePage(page->page_id_, page->data_);
            page->is_dirty_ = false;
        }
    }
}

void BufferPoolManager::update_page(Page *page, page_id_t new_page_id, frame_id_t new_frame_id) {
    if (page->IsDirty()) {
        disk_manager_->WritePage(page->page_id_, page->data_);
        page->is_dirty_ = false;
    }
    page_table_.erase(page->page_id_);
    if (new_page_id != INVALID_PAGE_ID) {
        page_table_.emplace(new_page_id, new_frame_id);
    }
    page->ResetMemory();
    page->page_id_ = new_page_id;
}

bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    return replacer_->Victim(frame_id);
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