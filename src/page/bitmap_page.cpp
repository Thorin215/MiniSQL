#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  bool IsSuccess = false;
  /*pages allocated are less than the supported size*/
  if(page_allocated_ < GetMaxSupportedSize()){
    this->page_allocated_++;
    /*Find the page to allocate*/
    while(!IsPageFree(this->next_free_page_)&& this->next_free_page_ < GetMaxSupportedSize()){
      this->next_free_page_++;
    }
    page_offset=this->next_free_page_;

    /*Byte Index*/
    uint32_t byte_index = this->next_free_page_/8;
    /*Bit Index*/
    uint8_t bit_index = this->next_free_page_%8;
    /*mark in the bitmap*/ 
    uint8_t tmp = 0x01;
    bytes[byte_index] = (bytes[byte_index]|(tmp<<(7-bit_index)));  
    /*point to next page*/
    while(!IsPageFree(this->next_free_page_)&&this->next_free_page_<GetMaxSupportedSize()) {
      this->next_free_page_++;
    }
    IsSuccess = true;
  }

  return IsSuccess;
}

/**
 * TODO: Student Implement
 */
template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  /*Byte Index*/
  uint32_t byte_index=page_offset/8;
  /*Bit Index*/
  uint8_t bit_index=page_offset%8;
  bool IsSuccess=false;
  /*Only deallocated when the page isn't free*/
  if( this->page_allocated_ && !IsPageFree(page_offset)){
    
    uint8_t tmp=0x01;

    bytes[byte_index]=bytes[byte_index]&(~(tmp<<(7-bit_index)));
    this->page_allocated_--;
    /*update the free page*/
    if(page_offset<this->next_free_page_)
      this->next_free_page_=page_offset;
    
    IsSuccess=true;
  }

  return IsSuccess;
}

/**
 * TODO: Student Implement
 */
template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  /*Byte Index*/
  uint32_t byte_index=page_offset/8;
  /*Bit Index*/
  uint8_t bit_index=page_offset%8;
  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  uint8_t tmp=0x01;
  uint8_t PageIsSuccess=(bytes[byte_index]&(tmp<<(7-bit_index)));
  if(bytes[byte_index]&(tmp<<(7-bit_index))) return false;
  else return true;
}


template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;