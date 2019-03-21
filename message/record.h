/*
 * record.h
 *
 *  Created on: 2018年12月12日
 *      Author: liwei
 */

#ifndef RECORD_H_
#define RECORD_H_
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
namespace DATABASE_INCREASE
{
struct recordHead
{
    uint64_t size;
    uint16_t headSize;
    uint8_t type;
    uint8_t version;
    uint16_t flag;
    uint32_t recordID;
    uint64_t logID;
    uint64_t logOffset;
    uint32_t timestamp;
    uint32_t timestampUSec;
};
#define UNSIGNED_COLUMN 0x01
#define GENRATED_COLUMN 0x02
#define PRIMARY_KEY_COLUMN 0x04
#define UNIQUE_KEY_COLUMN 0x08
enum RecordType
{
    INSERT,
    UPDATE,
    DELETE,
    REPLACE,
    BEGIN,
    COMMIT,
    ROLLBACK,
    DDL,
    HEARTBEAT
};
#define COLUMN_FLAG_VIRTUAL 0x01
struct columnDef
{
    uint8_t type;
    uint8_t flag;
    uint16_t charsetID;
    uint32_t size;
};
struct record
{
    const char * data;
    recordHead *head;
    void * userData;
    void * table;
    void * transaction;
    union{
        uint64_t * hash;
        void ** queue;
    };
    uint16_t hashCount;
    record(){}
    record(const char * data){
        this->data = data;
        head = (recordHead*)data;
        userData = NULL;
        table = NULL;
        hash = NULL;
        transaction = NULL;
        hashCount = 0;
    }
};
#define TEST_BITMAP(m,i) (((m)[(i)>>3]>>(i&0x7))&0x1)
#define SET_BITMAP(m,i)  (m)[(i)>>3]|= (0x01<<(i&0x7))
struct TableMetaMessage :public record
{
    /*--------------version 0----------------*/
    /*[64 bit tableMetaID][32 bit tableVersion][8 bit database size][database string+ 1 byte '\0'][8 bit table size][table string+ 1 byte '\0']
     * [8 bit charset size][charset string+ 1 byte '\0'][16 bit columnCount][columnDef 1]...[columnDef n]
     * [16 bit primaryKeyColumnCount][16 bit column id 1]...[16 bit column id n]
     * [16 bit uniqueKeyCount][16 bit uk 1 column count]...[16 bit uk n column count]
     *[16 bit uk 1 column 1 id ]...[16 bit uk 1 column n id ]
      * ...
      * [16 bit uk m column 1 id ]...[16 bit uk m column n id ]
     * */
    uint64_t tableMetaID;
    const char * database;
    const char * table;
    const char * charset;
    uint16_t columnCount;
    const columnDef *columns;
    uint16_t primaryKeyColumnCount;
    const uint16_t * primaryKeys;
    uint16_t uniqueKeyCount;
    const uint16_t * uniqueKeyColumnCounts;
    uint16_t ** uniqueKeys;
    /*-----------------------------------------------*/
    TableMetaMessage(const char * data):record(data){
        const char * ptr = data;
        tableMetaID = *(uint64_t*)(ptr+head->headSize);
        database = ptr+head->headSize+sizeof(tableMetaID)+1;
        table = database+*(uint8_t*)(database-1);
        charset = table+*(uint8_t*)(table-1);
        columnCount = *(uint16_t*)(charset+*(uint8_t*)(charset-1));
        columns = (columnDef*)(charset+*(uint8_t*)(charset-1)+sizeof(columnCount));
        ptr = (const char*)&columns[columnCount];
        assert((uint32_t)(ptr-data)<head->headSize+sizeof(primaryKeyColumnCount));
        primaryKeyColumnCount = *(uint16_t*)ptr;
        if(primaryKeyColumnCount !=0)
        {
            primaryKeys = (uint16_t*)(ptr+sizeof(primaryKeyColumnCount));
            ptr = (const char*)&primaryKeys[primaryKeyColumnCount];
        }
        else
        {
            primaryKeys = NULL;
            ptr += sizeof(primaryKeyColumnCount);
        }
        uniqueKeyCount = *(uint16_t*)ptr;
        if(uniqueKeyCount>0)
        {
            uniqueKeyColumnCounts = (uint16_t*)(ptr+sizeof(uniqueKeyCount));
            ptr = (const char*)&uniqueKeyColumnCounts[uniqueKeyCount];
            uniqueKeys = (uint16_t**)malloc(sizeof(uint16_t*)*uniqueKeyCount);
            for(uint16_t idx =0;idx<uniqueKeyCount;idx++)
            {
                uniqueKeys[idx] = (uint16_t*)ptr;
                ptr = (const char*)&uniqueKeys[idx][uniqueKeyColumnCounts[idx]];
            }
        }
        else
        {
            uniqueKeyColumnCounts = NULL;
            uniqueKeys = NULL;
        }
        /*if version increased in future,add those code:
          if(head->version >0)
          {do some thing}
          if(head->version >1)
          {do some thing}
             ...
           */
    }
    ~TableMetaMessage()
    {
        if(uniqueKeys!=NULL)
            free(uniqueKeys);
    }
};
struct DMLRecord:public record
{
    /*--------------version 0----------------*/
    /*[64 bit tableMetaID][32 bit tableVersion][16 bit columnCount]
     * [32 byte new column 1 size]...[32 byte new column n size] (if exist new column)
     * [new column 1+ 1byte '\0']...[new column n+ 1byte '\0']
     * [old column bit bitmap] (if update)
     * [32 byte old column 1 size]...[32 byte old column n size] (if exist old column)
     * [old column 1+ 1byte '\0']...[old column n+ 1byte '\0']
     * */
    struct DMLRecordHead{
    uint64_t tableMetaID;
    uint32_t tableVersion;
    uint16_t columnCount;
    };
    bool outerMem;
    DMLRecordHead *dmlHead;
    TableMetaMessage * meta;
    const char ** newColumns;
    uint32_t * oldColumnSizes;
    const char ** oldColumns;
    uint8_t *updatedBitmap;
    DMLRecord(){}
    /*----------------------------------------------*/
    DMLRecord(const char * data,void * mem = nullptr)  :
        record(data),outerMem(mem!=nullptr)
    {
        const char * ptr = data+head->headSize;
        dmlHead = (DMLRecordHead*)ptr;
        meta = NULL;
        ptr += sizeof(DMLRecordHead);
        newColumns = oldColumns = NULL;
        /*alloc mem one time*/
        if(mem == nullptr)
            mem = malloc(dmlHead->columnCount*sizeof(const char*)*(head->type==UPDATE||head->type==REPLACE)?2:1);
        if(head->type == INSERT||head->type == UPDATE)
        {
            ptr+=sizeof(uint32_t)*dmlHead->columnCount;
            newColumns = (const char **)mem;
            mem = (char*)mem+dmlHead->columnCount;
            for(uint16_t idx = 0;idx<dmlHead->columnCount;idx++)
            {
                newColumns[idx] = ptr;
            }
        }
        if(head->type == UPDATE)
        {
            updatedBitmap = (uint8_t*)ptr;
            ptr+= (dmlHead->columnCount>>3)+(dmlHead->columnCount&0x7f?1:0);
        }
        if(head->type == DELETE||head->type == UPDATE)
        {
            oldColumnSizes = (uint32_t*)ptr;
            ptr+=sizeof(uint32_t)*dmlHead->columnCount;
            oldColumns = (const char**)mem;
            for(uint16_t idx = 0;idx<dmlHead->columnCount;idx++)
            {
                oldColumns[idx] = ptr;
                ptr+= oldColumnSizes[idx];
            }
        }
      /*if version increased in future,add those code:
        if(head->version >0)
        {do some thing}
        if(head->version >1)
        {do some thing}
           ...
         */
    }
    inline const char* oldColumnOfUpdateType(uint16_t index)
    {
        if(TEST_BITMAP(updatedBitmap,index))
        {
            return oldColumns[index];
        }
        else
        {
            return newColumns[index];
        }
    }
    inline bool isKeyUpdated(const uint16_t * key,uint16_t keyColumnCount)
    {
        for(uint16_t idx=0;idx<keyColumnCount;idx++)
        {
            if(TEST_BITMAP(updatedBitmap,key[idx]))
                return true;
        }
        return false;
    }
    ~DMLRecord(){
        if(outerMem)
            return ;
        if(newColumns != NULL)
            free(newColumns);
        else if(oldColumns != NULL)//only newColumns is NULL,oldColumns not NULL ,free oldColumns
            free(oldColumns);
    }
};
struct DDLRecord :public record
{
    /*--------------version 0----------------*/
    /*[32 bit sqlMode]
     * [8 bit charset size][charset string + 1byte '\0']
     * [8 bit database size][database string + 1byte '\0']
     * [8 bit ddl size][ddl string + 1byte '\0']
     * */
    uint32_t sqlMode;
    const char * charset;
    const char * database;
    const char * ddl;
    /*----------------------------------------------*/
    DDLRecord(const char* data) :
            record(data)
    {
        const char * ptr = data;
        sqlMode = *(uint32_t*) (ptr + head->headSize);
        database = ptr + head->headSize + sizeof(sqlMode) + 1;
        ddl = database + *(uint8_t*) (database - 1);
        charset = ddl + *(uint8_t*) (ddl - 1);
        /*if version increased in future,add those code:
          if(head->version >0)
          {do some thing}
          if(head->version >1)
          {do some thing}
             ...
           */
    }
};
}




#endif /* RECORD_H_ */
