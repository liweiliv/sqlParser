/*
 * transaction.h
 *
 *  Created on: 2018年12月14日
 *      Author: liwei
 */
#include <stdlib.h>
#include "record.h"
#define __cplusplus  201103L
#include <atomic>
namespace DATABASE_INCREASE
{
struct transaction{
    void * userData;
    transaction * next;
    std::atomic<uint32_t> blockingRecordCount;
    uint16_t recordCount;
    record *records[1];
    inline DMLRecord *DML(uint16_t index)
    {
        return records[index]->head->type<=REPLACE?static_cast<DMLRecord*>(records[index]):NULL;
    }
    inline DDLRecord *DDL(uint16_t index)
    {
        return records[index]->head->type==DDL?static_cast<DDLRecord*>(records[index]):NULL;
    }
    ~transaction()
    {
        for(uint16_t idx=0;idx<recordCount;idx++)
        {
            delete records[idx];
        }
    }
};
}



