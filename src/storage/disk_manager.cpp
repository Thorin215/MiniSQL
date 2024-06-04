#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
    std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    // directory or file does not exist
    if (!db_io_.is_open()) {
        db_io_.clear();
        // create a new file
        std::filesystem::path p = db_file;
        if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
        db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
        db_io_.close();
        // reopen with original mode
        db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
        if (!db_io_.is_open()) {
            throw std::exception();
        }
    }
    ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
    std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
    WritePhysicalPage(META_PAGE_ID, meta_data_);
    if (!closed) {
        db_io_.close();
        closed = true;
    }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
    //ASSERT(logical_page_id >= 0, "Invalid page id.");
    ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
    //ASSERT(logical_page_id >= 0, "Invalid page id.");
    WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::ReadBitMapPage(page_id_t extent_id, char *page_data) {
    //int extent_id=logical_page_id/BIT_MAP_SIZE;
    //Page_data will record the data read from the disk
    page_id_t BitMap_Index=extent_id*(BITMAP_SIZE+1)+1;
    //Read the Bit map from the disk
    ReadPhysicalPage(BitMap_Index,page_data);
}

page_id_t DiskManager::AllocatePage() {
    /*Read the meta data*/
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);
    /*assign the next page to return*/
    uint32_t NextPage=0;
    bool IsSuccess = false;

    meta_page->num_allocated_pages_++;
    bool NewOpen = true;
    uint32_t i;
    for (i = 0; i < meta_page->num_extents_; i++){
        if (meta_page->extent_used_page_[i] < BITMAP_SIZE){
            NewOpen = false;
            break;
        }
    }

    if(NewOpen){
        i = meta_page->num_extents_++;
        meta_page->extent_used_page_[i]++;
    }else{
        meta_page->extent_used_page_[i]++;
    }

    char Page_Data[PAGE_SIZE];
    ReadBitMapPage(i,Page_Data);

    BitmapPage<PAGE_SIZE> *Bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);
    IsSuccess = Bitmap_page->AllocatePage(NextPage);

    if(IsSuccess){
        page_id_t BitMap_page_id=i*(BITMAP_SIZE+1)+1;
        char *Page_Data = reinterpret_cast<char *>(Bitmap_page);
        WritePhysicalPage(BitMap_page_id,Page_Data);
        NextPage += i*BITMAP_SIZE;
    }
    return NextPage;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);

    page_id_t Physical_Page_Id = this->MapPageId(logical_page_id);
    if(!meta_page->GetExtentNums()) return ;
    bool IsSuccess = false;
    int extent_id=logical_page_id/BITMAP_SIZE;

    char Init_Page_Data[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; i++) {
        Init_Page_Data[i] = 0x00;
    }
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[extent_id]--;
    char Page_Data[PAGE_SIZE];
    ReadBitMapPage(extent_id,Page_Data);
    BitmapPage<PAGE_SIZE> *Bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);

    IsSuccess = Bitmap_page->DeAllocatePage(logical_page_id%BITMAP_SIZE);
    if(IsSuccess){
        /*success*/
        page_id_t BitMap_page_id=extent_id*(BITMAP_SIZE+1)+1;
        char *Page_Data = reinterpret_cast<char *>(Bitmap_page);
        WritePhysicalPage(BitMap_page_id, Page_Data);
        /*cover the physical page*/
        WritePhysicalPage(Physical_Page_Id, Init_Page_Data);
    }else{
        /*fail*/
        WritePhysicalPage(Physical_Page_Id, Init_Page_Data);
        return;
    }
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
    char Page_Data[PAGE_SIZE];

    ReadBitMapPage(logical_page_id/BITMAP_SIZE, Page_Data);

    BitmapPage<PAGE_SIZE> * bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);
    bool IsSuccess=bitmap_page->IsPageFree(logical_page_id%BITMAP_SIZE);
    return IsSuccess;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
    return logical_page_id/BITMAP_SIZE+2+logical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
    int offset = physical_page_id * PAGE_SIZE;
    // check if read beyond file length
    if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "Read less than a page" << std::endl;
#endif
        memset(page_data, 0, PAGE_SIZE);
    } else {
        // set read cursor to offset
        db_io_.seekp(offset);
        db_io_.read(page_data, PAGE_SIZE);
        // if file ends before reading PAGE_SIZE
        int read_count = db_io_.gcount();
        if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
            LOG(INFO) << "Read less than a page" << std::endl;
#endif
            memset(page_data + read_count, 0, PAGE_SIZE - read_count);
        }
    }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
    size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
    // set write cursor to offset
    db_io_.seekp(offset);
    db_io_.write(page_data, PAGE_SIZE);
    // check for I/O error
    if (db_io_.bad()) {
        LOG(ERROR) << "I/O error while writing";
        return;
    }
    // needs to flush to keep disk file in sync
    db_io_.flush();
}