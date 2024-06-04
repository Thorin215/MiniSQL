#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
    uint32_t offset=0;
    uint32_t unit=sizeof(uint32_t);

    MACH_WRITE_UINT32(buf,COLUMN_MAGIC_NUM);
    offset+=unit;
    //write name length
    MACH_WRITE_UINT32(buf+offset,name_.length());
    offset+=unit;
    //write name
    MACH_WRITE_STRING(buf+offset,name_);
    offset+=name_.length();
    //write type
    MACH_WRITE_TO(TypeId,buf+offset,type_);
    offset+=sizeof(TypeId);
    //write len
    MACH_WRITE_UINT32(buf+offset,len_);
    offset+=unit;
    //write table_ind
    MACH_WRITE_UINT32(buf+offset,table_ind_);
    offset+=unit;
    //write nullable
    memcpy(buf+offset, &nullable_, sizeof(nullable_));
    offset+=sizeof(nullable_);
    //write unique
    memcpy(buf+offset, &unique_, sizeof(unique_));
    offset+=sizeof(unique_);

    return offset;

}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
    return sizeof(COLUMN_MAGIC_NUM)+sizeof(uint32_t)+name_.length()+sizeof(type_)+sizeof(len_)+sizeof(table_ind_)+sizeof(nullable_)+sizeof(unique_);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
    uint32_t COLUMN_MAGIC_NUM_ref;
    uint32_t offset=0;
    memcpy(&COLUMN_MAGIC_NUM_ref, buf, sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    if(COLUMN_MAGIC_NUM_ref!=COLUMN_MAGIC_NUM){
        return 0;
    }

    uint32_t nameLength;
    memcpy(&nameLength, buf+offset, sizeof(uint32_t));
    offset+=sizeof(uint32_t);

    std::string name;
    name.resize(nameLength);
    memcpy(&name[0], buf+offset, nameLength);
    offset+=nameLength;

    TypeId type;
    memcpy(&type, buf+offset, sizeof(type));
    offset+=sizeof(type);

    uint32_t len;
    memcpy(&len, buf+offset, sizeof(len));
    offset+=sizeof(len);

    uint32_t table_ind;
    memcpy(&table_ind, buf+offset, sizeof(table_ind));
    offset+=sizeof(table_ind);

    bool nullable;
    memcpy(&nullable, buf+offset, sizeof(nullable));
    offset+=sizeof(nullable);

    bool unique;
    memcpy(&unique, buf+offset, sizeof(unique));
    offset+=sizeof(unique);

    if(type==TypeId::kTypeChar) { //字符类型的列需要指定长度，整数或浮点数类型的列不需要。
        column=new Column(name, type, len, table_ind, nullable, unique);
    }
    else{
        column=new Column(name, type, table_ind, nullable, unique);
    }

    return offset;
}
