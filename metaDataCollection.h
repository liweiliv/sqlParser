/*
 * metaDataCollection.h
 *
 *  Created on: 2018年12月3日
 *      Author: liwei
 */

#ifndef METADATACOLLECTION_H_
#define METADATACOLLECTION_H_

#include <string>
#include "trieTree.h"
#include <unorderMapUtil.h>
#include "skiplist.h"
struct tableMeta;
struct columnMeta;
namespace sqlParser
{
class sqlParser;
class newColumnInfo;
class newTableInfo;
struct handle;
struct Table;
struct databaseInfo;
}
typedef std::tr1::unordered_map<const char *,uint32_t,StrHash,StrCompare> CharsetTree ;
struct tableMetaWrap
{
    uint64_t tableID;
    tableMeta * meta;
};
struct tableIDComparator
{
    int operator()(const tableMetaWrap *& a, const tableMetaWrap *& b) const
    {
        if (a->tableID < b->tableID)
        {
            return -1;
        }
        else if (a->tableID > b->tableID)
        {
            return +1;
        }
        else
        {
            return 0;
        }
    }
};
class metaDataCollection
{
private:
    trieTree m_dbs;
    CharsetTree m_charsetSizeList;
    std::string m_defaultCharset;
    leveldb::SkipList<tableMetaWrap*, tableIDComparator> m_allTables;
    sqlParser::sqlParser * m_SqlParser;
    uint64_t m_maxTableId;
public:
    metaDataCollection();
    ~metaDataCollection();
    tableMeta * get(uint64_t tableID);
    tableMeta * get(const char * database,const char * table,uint64_t originCheckPoint);
    int put(const char * database,const char * table,tableMeta * meta,uint64_t originCheckPoint);
    int purge(uint64_t originCheckPoint);
    int processDDL(const char * ddl,uint64_t originCheckPoint);
private:
    int createTable(sqlParser::handle * h,const sqlParser::newTableInfo *t,uint64_t originCheckPoint);
    int createTableLike(sqlParser::handle * h,const sqlParser::newTableInfo *t,uint64_t originCheckPoint);
    int alterTable(sqlParser::handle * h,const sqlParser::newTableInfo *t,uint64_t originCheckPoint);
    int processNewTable(sqlParser::handle * h,const sqlParser::newTableInfo *t,uint64_t originCheckPoint);
    int processOldTable(sqlParser::handle * h,const sqlParser::Table *table,uint64_t originCheckPoint);
    int processDatabase(const sqlParser::databaseInfo * database,uint64_t originCheckPoint);
public:
};
#endif /* METADATACOLLECTION_H_ */
