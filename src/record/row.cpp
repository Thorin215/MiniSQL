#include "record/row.h"
#include<string>

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    // replace with your code here

    //Header implement
    uint32_t ret=0;
    uint32_t fieldCount=this->GetFieldCount();
    //uint32_t fieldCount=schema->GetColumnCount();
    MACH_WRITE_UINT32(buf,fieldCount);
    ret+=sizeof(uint32_t);

    //std::cout<<fieldCount<<std::endl;

    std::string bitMap;
    //firstly initialize the bitmap
    for(uint32_t i=0;i<fieldCount;i++)
    {
        if(fields_[i]->IsNull()){
            bitMap.push_back('\1');
        }else{
            bitMap.push_back('\0');
        }
    }
    MACH_WRITE_STRING(buf+ret,bitMap);
    ret+=bitMap.length();

    //content implement
    for(uint32_t i=0;i<fieldCount;i++)
    {
        if(fields_[i]->IsNull()==0){
            ret+=fields_[i]->SerializeTo(buf+ret); //Serialize every single field using method in field
        }
    }

    ASSERT(MACH_READ_FROM(uint32_t,(buf))==schema->GetColumnCount(),"fuck");
    return ret;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    // replace with your code here
    // replace with your code here
    fields_.clear(); // !!! caution

    if(buf== nullptr){
        return 0;
    }

    uint32_t ret=0;
    uint32_t fieldCount= MACH_READ_FROM(uint32_t,(buf)); //get the number of fields

    ASSERT(fieldCount==schema->GetColumnCount(),"count not match");
    ret+=sizeof(uint32_t);

    //get the bitmap
    std::string bitMap;
    for(uint32_t i=0;i<fieldCount;i++)
    {
        bitMap.push_back(*(buf+ret+i));
    }
    ret+=fieldCount;

    for(uint32_t i=0;i<fieldCount;i++)
    {
        TypeId type_id=schema->GetColumn(i)->GetType();
        bool isNull;
        if(bitMap[i]=='\1'){
            isNull=true;
        }else{
            isNull=false;
        }
        Field* field= nullptr;
        ret+=field->DeserializeFrom(buf+ret,type_id,&field,isNull); //note that the method returns 0 when null
        fields_.push_back(field);
    }

    //ASSERT(fields_.size()==schema->GetColumnCount(),"deserialize field wrong");
    return ret;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
    //ASSERT(schema != nullptr, "Invalid schema before serialize.");
    //ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    //replace with your code here

    uint32_t ret=0;

    //for field count
    ret+=sizeof(uint32_t);

    //add the size of bitMap
    ret+=this->GetFieldCount();

    //3.Calculate the Size of the Field
    for(uint32_t i=0;i<this->GetFieldCount();i++)
    {
        if(!fields_[i]->IsNull()){
            ret+=fields_[i]->GetSerializedSize();
        }
    }

    return ret;
}


void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
    auto columns = key_schema->GetColumns();
    std::vector<Field> fields;
    uint32_t idx;
    for (auto column : columns) {
        schema->GetColumnIndex(column->GetName(), idx);
        fields.emplace_back(*this->GetField(idx));
    }
    key_row = Row(fields);
}
