#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
    uint32_t offset=0;

    MACH_WRITE_UINT32(buf,SCHEMA_MAGIC_NUM);
    offset+=sizeof(uint32_t);
    //write size of columns
    MACH_WRITE_UINT32(buf+offset,columns_.size());
    offset+=sizeof(uint32_t);

    for(auto &column : columns_)
    {
        offset+=column->SerializeTo(buf+offset);
    }

    memcpy(buf+offset, &is_manage_, sizeof(bool));
    offset+=sizeof(bool);

    return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here

    uint32_t offset=0;
    offset=sizeof(SCHEMA_MAGIC_NUM)+sizeof(uint32_t);

    for(auto &column : columns_)
    {
        offset+=column->GetSerializedSize();
    }
    offset+=sizeof(is_manage_);
    return offset;

}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) { //or tp say Schema* &schema
  // replace with your code here

    uint32_t offset=0;
    uint32_t SCHEMA_MAGIC_NUM_ref;
    uint32_t columnCount;

    memcpy(&SCHEMA_MAGIC_NUM_ref, buf+offset, sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    if(SCHEMA_MAGIC_NUM_ref!=SCHEMA_MAGIC_NUM){
        return 0;
    }

    memcpy(&columnCount, buf+offset, sizeof(uint32_t));
    offset+=sizeof(uint32_t);

    std::vector<Column *>columns;
    for(uint32_t i=0;i<columnCount;i++)
    {
        Column *temp;
        offset+=Column::DeserializeFrom(buf+offset, temp); //temp will be modified
        columns.push_back(temp);
    }

    bool is_manage;
    memcpy(&is_manage, buf+offset, sizeof(bool));
    offset+=sizeof(bool);

    schema = new Schema(columns, is_manage); //input reference parameter is modified
    return offset;
}