<div class="cover" style="page-break-after:always;font-family:方正公文仿宋;width:100%;height:100%;border:none;margin: 0 auto;text-align:center;">
    <div style="width:100%;margin: 0 auto;height:0;padding-bottom:10%;">
        </br>
        <img src="https://raw.githubusercontent.com/Keldos-Li/pictures/main/typora-latex-theme/ZJU-name.svg" alt="校名" style="width:60%;"/>
    </div>
    </br></br></br></br></br>
    <div style="width:60%;margin: 0 auto;height:0;padding-bottom:40%;">
        <img src="https://raw.githubusercontent.com/Keldos-Li/pictures/main/typora-latex-theme/ZJU-logo.svg" alt="校徽" style="width:60%;"/>
    </div>
<font size = 59, style="width:40%;font-weight:normal;text-align:center;font-family:华文仿宋"> 数据库系统实验报告 </font>
    </br>
    </br>
</br></br></br></br></br>
    <table style="border:none;text-align:center;width:72%;font-family:仿宋;font-size:14px; margin: 0 auto;">
    <tbody style="font-family:方正公文仿宋;font-size:12pt;">
        <tr style="font-weight:normal;"> 
            <td style="width:20%;text-align:right;">题　　目</td>
            <td style="width:2%">：</td> 
            <td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">MiniSQL</td>     </tr>
        <tr style="font-weight:normal;"> 
            <td style="width:20%;text-align:right;">授课教师</td>
            <td style="width:2%">：</td> 
            <td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">孙建伶</td>     </tr>
         <tr style="font-weight:normal;"> 
            <td style="width:20%;text-align:right;">助　　教</td>
            <td style="width:2%">：</td> 
            <td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">聂俊哲/石宇新</td>     </tr>
        <tr style="font-weight:normal;"> 
            <td style="width:20%;text-align:right;">姓　　名</td>
            <td style="width:2%">：</td> 
            <td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">王淳</td>     </tr>
        <tr style="font-weight:normal;"> 
            <td style="width:20%;text-align:right;">学　　号</td>
            <td style="width:2%">：</td> 
            <td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">3220105023</td>     </tr>
         <tr style="font-weight:normal;"> 
            <td style="width:10%;text-align:right;">邮　　箱</td>
            <td style="width:2%">：</td> 
            <td style="width:100%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">zjuheadmaster@zju.edu.cn</td>     </tr>
         <tr style="font-weight:normal;"> 
            <td style="width:20%;text-align:right;">联系电话</td>
            <td style="width:2%">：</td> 
            <td style="width:40%;font-weight:normal;border-bottom: 1px solid;text-align:center;font-family:华文仿宋">13428807817</td>     </tr>
</tbody>              
</table>
</div>



<font size = 8> Contents </font>

[toc]

## DISK AND BUFFER POOL MANAGER

### 位图页的实现

- 这个部分实现`AllocatePage`,`DeAllocatePage`,`IsPageFree`,`IsPageFreeLow`四个函数，支持对位图页的分配以及删除，还有对其状态的检查。

#### AllocatePage

```cpp
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
```

- 在确定位图中的`Byte Index`以及`Bit Index`之前，要先判断是否存在额外的空余页可供处理，若有则找到该页，如果没有则返回`False`，若有，则确定该页的`Byte Index`以及`Byte Index`，更新`Bitmap`后将新开的`Page`的偏移地址传回。

#### DeAllocatePage

```cpp
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
```

- 这里先检查传入要释放的页偏移地址的空间是否被使用，若无，则返回`False`，否则更新`bitmap`以及更新`next_free_page_`指针。

#### IsPageFree

```cpp
template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  /*Byte Index*/
  uint32_t byte_index=page_offset/8;
  /*Bit Index*/
  uint8_t bit_index=page_offset%8;
  return IsPageFreeLow(byte_index, bit_index);
}
```

- 通过检查`bitmap`中对应的位来确定是否为`Free page`.

#### IsPageFreeLow

```cpp
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  uint8_t tmp=0x01;

  if(bytes[byte_index]&(tmp<<(7-bit_index))) return false;
  else return true;
}
```

- 这个函数实现了根据输入的`bit_index`以及`byte_index`来判断是否为空页。

### 磁盘数据页管理

- `DiskManager::AllocatePage()`：从磁盘中分配一个空闲页，并返回空闲页的**逻辑页号**；
- `DiskManager::DeAllocatePage(logical_page_id)`：释放磁盘中**逻辑页号**对应的物理页。
- `DiskManager::IsPageFree(logical_page_id)`：判断该**逻辑页号**对应的数据页是否空闲。
- `DiskManager::MapPageId(logical_page_id)`：可根据需要实现。在`DiskManager`类的私有成员中，该函数可以用于将逻辑页号转换成物理页号。



- `GetExtentNums()`: 返回区间数。
- `GetAllocatedPages()`: 返回已分配的页面数。
- `GetExtentUsedPage(uint32_t extent_id)`: 返回指定区间已使用的页面数，如果区间ID超出范围，返回0。

- `num_allocated_pages_`: 已分配页面数。
- `num_extents_`: 区间数，每个区间由一个位图和若干页面组成。
- `extent_used_page_`: 每个区间已使用的页面数。





#### AllocatePage

`AllocatePage`函数用于在磁盘上分配新的页面，并更新相关的元数据。具体步骤包括更新磁盘文件元数据页、查找非满的区间（extent）、读取位图页、分配新的页面并写回磁盘。



```cpp
page_id_t DiskManager::AllocatePage() {
  /*Read the meta data*/
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);
  /*assign the next page to return*/
  uint32_t NextPage=0;
  bool IsSuccess = false;
  /*New disk*/
  if(!meta_page->GetExtentNums()){
    /*extents*/
    meta_page->num_extents_++;
    /*pages*/
    meta_page->num_allocated_pages_++;
    /*used page*/
    meta_page->extent_used_page_[0] = 1;
    /*read the bitmap data from disk*/
    char Page_Data[PAGE_SIZE];
    ReadPhysicalPage(1,Page_Data);
    BitmapPage<PAGE_SIZE> *Bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);

    IsSuccess = Bitmap_page->AllocatePage(NextPage);
    if(IsSuccess){
      char *Page_Data = reinterpret_cast<char *>(Bitmap_page);
      WritePhysicalPage(1,Page_Data);
    }else{
      std::cout << "AllocatePage Failed  ----- at the begin!" << std::endl;
    }
  }else{
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
    }else{
      std::cout << "AllocatePage Failed  ----- at the middle!" << std::endl;
    }
  }
  return NextPage;
}
```



#### DeAllocatePage



```cpp
```





#### IsPageFree





#### MapPageId





### LRU替换策略





### 缓冲池管理



