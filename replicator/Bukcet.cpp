/*
 * Bukcet.cpp
 *
 *  Created on: 2018年12月14日
 *      Author: liwei
 */
#include <string.h>
#include <string>
#include <stdint.h>
#include <stdlib.h>
#include <map>
#include <assert.h>
#include <tr1/unordered_map>
#include "sqlParser.h"
#include "transaction.h"
#include <pthread.h>
#include "skiplist.h"
#include "crcBySSE.h"
#include <atomic>
using namespace std;
namespace DATABASE_INCREASE
{
static inline int64_t stringHash(const char* str,uint32_t size)
{
    unsigned int hash = 1315423911;
    const char * ptr = str;
    while(ptr<str+size) {
        if (*ptr >= 'A' && *ptr <= 'Z')
            hash ^= ((hash << 5) + (*ptr++ + ('a' - 'A')) + (hash >> 2));
        else
            hash ^= ((hash << 5) + (*ptr++) + (hash >> 2));
    }
    return (hash & 0x7FFFFFFF);
}
class StrHash
{
public:
    static int64_t operator()(const char* str) const
    {
        unsigned int hash = 1315423911;
        while(*str) {
            if (*str >= 'A' && *str <= 'Z')
                hash ^= ((hash << 5) + (*str++ + ('a' - 'A')) + (hash >> 2));
            else
                hash ^= ((hash << 5) + (*str++) + (hash >> 2));
        }
        return (hash & 0x7FFFFFFF);
    }

};
class RecordQueue
{
    struct msgQueueNode
    {
        struct msgQueueNode * next;
        char * buf;
        char * start;
        char * end;
        size_t size;
    };
};
class IgnoreCaseComparator
{
public:
    inline bool operator()(const char* s1,const char* s2) const
    {
        return (strcasecmp(s1, s2) == 0);
    }
};

struct KeyInfo
{
    uint8_t KeySize;
    uint16_t *KeyIndexs;
    KeyInfo():KeySize(0),KeyIndexs(NULL){}
    ~KeyInfo(){if(KeyIndexs) free(KeyIndexs);}
};
struct HASH_KEY
{
    uint64_t hash;
    BSSQueue<record*> * queue;
    HASH_KEY(uint64_t _hash,BSSQueue<record*> * _queue):hash(_hash),queue(_queue){}
    HASH_KEY(const HASH_KEY & source):hash(source.hash),queue(source.queue){}
};
struct Comparator {
  inline int operator()(const HASH_KEY *& a, const HASH_KEY *& b) const {
    if (a->hash < b->hash) {
      return -1;
    } else if (a->hash > b->hash) {
      return +1;
    } else {
      return 0;
    }
  }
};
struct BTableVersion
{
    TableMetaMessage * meta;
    leveldb::SkipList<HASH_KEY*,Comparator> queueMap;
    BTableVersion * next;
    transaction * ddl;
    uint32_t records;
    BTableVersion():meta(NULL),next(NULL),ddl(NULL),records(0)
    {
    }
    ~BTableVersion()
    {
        if(meta)
            delete meta;
    }
    inline void lock()
    {
    }
    inline void unlock()
    {
    }
};
struct BTable
{
    string name;
    bool merge;
    BTableVersion * current;
};
/* In order to access data security, map indexes use string instead of char * */
typedef std::tr1::unordered_map<const char*, BTable*, StrHash, IgnoreCaseComparator> NameTbaMap;
struct BDatabase
{
    string name;
    NameTbaMap tables;
};
/* In order to access data security, map indexes use string instead of char * */
typedef std::tr1::unordered_map<const char*, BDatabase*, StrHash, IgnoreCaseComparator> NameDBMap;
class Bucket
{
private:
    NameDBMap m_dbs;
    bool m_allowDDLConcurrency;
    sqlParser::sqlParser m_sqlParser;
   // BinlogFilter *m_mergeTables;
    BTable * getTableOfRecord(const char * dbName,const char* tableName)
    {
        BTable * table = NULL;
        BDatabase * db = NULL;
        NameDBMap::iterator _db = m_dbs.find(dbName);
        if(_db == m_dbs.end())
        {
            db = new BDatabase();
            db->name = dbName;
            assert(m_dbs.insert(pair<const char * ,BDatabase*>(db->name.c_str(),db)).second);
        }
        else
            db = _db->second;
        NameTbaMap::iterator _table = db->tables.find(tableName);
        if(_table == db->tables.end())
        {
            table = new BTable();
            table->name = tableName;
            //table->merge = m_mergeTables->isTableRequired(db->name,table->name); todo
            table->current = new BTableVersion();
            assert(db->tables.insert(pair<const char * ,BTable*>(table->name.c_str(),table)).second);
        }
        else
            table = _table->second;
        return table;
    }
    BSSQueue<record*> * getQueueByHash(BTableVersion * table,uint64_t hash)
    {
        HASH_KEY k(hash,NULL);
        leveldb::SkipList<HASH_KEY*, Comparator>::Iterator iter(&table->queueMap);
        if(iter.Seek(&k))
        {
            return iter.key()->queue;
        }
        else
        {
            HASH_KEY * newQueue = new HASH_KEY(hash,new BSSQueue<record*>());
            newQueue->queue->userData = (void*)hash;
            table->queueMap.Insert(newQueue);
            return newQueue->queue;
        }
    }

    void getRecordHash(BTableVersion * table ,DMLRecord * record)
    {
        record->hashCount = (table->meta->primaryKeyColumnCount>0?1:0)+table->meta->uniqueKeyCount;
        if(record->head->type == UPDATE)
            record->hashCount*=2;
        if(record->hashCount == 0)
        {
            record->hash = NULL;
            return ;
        }
        record->hash = (uint64_t*)malloc(sizeof(uint64_t)*record->hashCount);
        uint16_t keyIdx = 0;
        if(record->head->type == INSERT||record->head->type == UPDATE)
        {
            record->hash[keyIdx] = 0;
            for(uint16_t idx =0;idx<table->meta->primaryKeyColumnCount;idx++)
            {
                record->hash[keyIdx] = hwCrc32c(record->hash[keyIdx],
                        record->newColumns[table->meta->primaryKeys[idx]],
                        record->newColumnSizes[table->meta->primaryKeys[idx]]-1);
                keyIdx++;
            }
            for(uint16_t idx = 0;idx<table->meta->uniqueKeyCount;idx++)
            {
                record->hash[keyIdx] = 0;
                for(uint16_t n =0;n<table->meta->uniqueKeyColumnCounts[idx];n++)
                {
                    record->hash[keyIdx] = hwCrc32c(record->hash[keyIdx],
                            record->newColumns[table->meta->uniqueKeys[idx][n]],
                            record->newColumnSizes[table->meta->uniqueKeys[idx][n]]-1);
                    keyIdx++;
                }
            }
        }
        if(record->head->type == UPDATE||record->head->type == DELETE)
        {
            record->hash[keyIdx] = 0;
            for(uint16_t idx =0;idx<table->meta->primaryKeyColumnCount;idx++)
            {
                record->hash[keyIdx] = hwCrc32c(record->hash[keyIdx],
                        record->oldColumns[table->meta->primaryKeys[idx]],
                        record->oldColumnSizes[table->meta->primaryKeys[idx]]-1);
            }

            if(record->head->type != UPDATE||record->hash[keyIdx]!=record->hash[0])
                keyIdx++;
            else
                record->hash[keyIdx] = 0;

            for(uint16_t idx = 0;idx<table->meta->uniqueKeyCount;idx++)
            {
                record->hash[keyIdx] = 0;
                for(uint16_t n =0;n<table->meta->uniqueKeyColumnCounts[idx];n++)
                {
                    record->hash[keyIdx] = hwCrc32c(record->hash[keyIdx],
                            record->oldColumns[table->meta->uniqueKeys[idx][n]],
                            record->oldColumnSizes[table->meta->uniqueKeys[idx][n]]-1);
                }
                if(record->head->type != UPDATE||record->hash[keyIdx]!=record->hash[table->meta->primaryKeyColumnCount>0?idx+1:idx])
                    keyIdx++;
                else
                    record->hash[keyIdx] = 0;
            }
        }
        record->hashCount = keyIdx;
    }
    inline void processKey(BTableVersion * table ,record * r,uint16_t idx,uint32_t &blockCounts)
    {
        BSSQueue<record*> * queue = getQueueByHash(table,r->hash[idx]);
        assert(queue);
        queue->push(r);
        if(queue->head()!=r)
            blockCounts++;
    }
    int initMeta(BTableVersion * table,uint64_t metaId)
    {
        return 0;
    }
    int processTableMeta(TableMetaMessage * meta)
    {
        return 0;
    }
    int processDML(transaction * trans,DMLRecord * record,uint32_t &blockCounts)
    {
        BTableVersion * table = getTableOfRecord(record->meta->database, record->meta->table)->current;
        record->table = table;
        if(table->meta == NULL && initMeta(table,record->tableMetaID)!=0)
        {
            //todo
            return -1;
        }
        getRecordHash(table,record);
        if(record->hash == NULL)
        {
            processKey(table,record,0,blockCounts);
            return 0;
        }
        for(uint16_t idx = 0;idx<record->hashCount;idx++)
        {
            processKey(table,record,idx,blockCounts);
        }
        return 0;
    }
    int processDDL(transaction * trans,DDLRecord * record,uint32_t &blockCounts)
    {
        sqlParser::handle * h = NULL;
        if(sqlParser::OK!=m_sqlParser.parse(h,record->ddl))
        {
            //todo
            return -1;
        }
        string database ;
        sqlParser::handle * handle = h;
        while(handle!=NULL)
        {
            if(handle->dbName.empty())
                handle->dbName = record->database;
            if(handle->meta.database.type!=sqlParser::databaseInfo::MAX_DATABASEDDL_TYPE)
            {
                //todo
            }
            else
            {
                for (list<sqlParser::newTableInfo*>::const_iterator it = handle->meta.newTables.begin(); it != handle->meta.newTables.end(); it++)
                {
                    if(!m_allowDDLConcurrency)
                    {
                        BTable * t = getTableOfRecord("_BUCKET_DDL_DATABASE_","_BUCKET_DDL_TABLE_");
                        processKey(t->current,record,1,blockCounts);
                    }
                    if((*it)->table.database.empty())
                    {
                        if(handle->dbName.empty())
                        {
                            //todo
                            return -1;
                        }
                        database = handle->dbName;
                    }
                    else
                        database = (*it)->table.database;
                    BTable * table = getTableOfRecord(database.c_str(),(*it)->table.table.c_str());
                    if(!table->current->queueMap.empty())
                    {
                        BTableVersion * newTable = new BTableVersion;
                        newTable->ddl = record;
                        table->current->next = newTable;
                        table->current = newTable;
                        blockCounts++;
                    }
                }

                for (list<sqlParser::Table>::const_iterator it = handle->meta.oldTables.begin(); it != handle->meta.oldTables.end(); it++)
                {
                    if(!m_allowDDLConcurrency)
                    {
                        BTable * t = getTableOfRecord("_BUCKET_DDL_DATABASE_","_BUCKET_DDL_TABLE_");
                        processKey(t->current,record,1,blockCounts);
                    }
                    if((*it).database.empty())
                    {
                        if(handle->dbName.empty())
                        {
                            //todo
                            return -1;
                        }
                        database = handle->dbName;
                    }
                    else
                        database = (*it).database;
                    BTable * table = getTableOfRecord(database.c_str(),(*it).table.c_str());
                    if(!table->current->queueMap.empty())
                    {
                        BTableVersion * newTable = new BTableVersion;
                        newTable->ddl = record;
                        table->current->next = newTable;
                        table->current = newTable;
                        blockCounts++;
                    }
                }
            }
            handle = handle->next;
        }
        return 0;
    }
    void schedule(transaction * trans)
    {

    }
    int writer(transaction * trans)
    {
        transaction *s = trans;
        //todo query
        while(s!=NULL)
        {
            for(int idx = 0;idx<s->recordCount;idx++)
            {
                record * r = s->records[idx];
                BTableVersion * table = static_cast<BTableVersion*>(r->table);
                uint8_t type = s->records[idx]->head->type;
                if(type<=REPLACE)
                {
                    BSSQueue<record*> ** queue = static_cast<BSSQueue<record*>**>(r->queue);
                    for(int16_t idx = (table->meta->primaryKeyColumnCount==0?0:1)+table->meta->uniqueKeyCount-1;idx>=0;idx--)
                    {
                        queue[idx]->pop();
                        record * prev = static_cast<record*>(queue[idx]->get());
                        if(prev != NULL)
                        {
                            transaction * prevTrans = static_cast<transaction*>(prev->transaction);
                            if(--prevTrans->blockingRecordCount==0)
                                schedule(prevTrans);
                        }
                        else
                        {
                            table->lock();
                            if(queue[idx]->empty())
                                table->queueMap.erase((uint64_t)queue[idx]->userData);
                            table->unlock();
                        }
                    }
                }
                else if(type == DDL)
                {
                    table->ddl = NULL;
                    if(table->next)
                    {
                        table->next->lock();
                        for(map<uint64_t,BSSQueue<record*>*>::iterator iter = table->next->queueMap.begin();iter!=table->next->queueMap.end();iter++)
                        {
                            record * r = static_cast<record*>(iter->second->get());
                            if(r!=NULL)
                            {
                                if(--static_cast<transaction*>(r->transaction)->blockingRecordCount == 0)
                                    schedule(static_cast<transaction*>(r->transaction));
                            }
                        }
                    }
                }
                if(table->next != NULL)
                {
                    if(table->queueMap.empty())//todo
                    {
                        transaction * ddl = static_cast<transaction*>(table->next->ddl->transaction);
                        delete table;
                        if(0==--ddl->blockingRecordCount)
                            schedule(ddl);
                    }
                }
            }
            s = s->next;
        }
        return 0;
    }
    int put(transaction * trans)
    {
        uint32_t blockCounts = 0;
        trans->blockingRecordCount.store(0x7fffffffu);
        for(uint32_t idx  = 0;idx<trans->recordCount;idx++)
        {
            if(trans->records[idx]->head->type<=REPLACE)
                processDML(trans,trans->DML(idx),blockCounts);
            else if(trans->records[idx]->head->type == DDL)
                processDDL(trans,trans->DDL(idx),blockCounts);
            else if(trans->records[idx]->head->type != HEARTBEAT)
                return -1;
        }
        trans->blockingRecordCount += blockCounts;
        if(trans->blockingRecordCount.fetch_sub(0x7fffffff)==0x7fffffff)
            schedule(trans);
        return 0;
    }
};
}




