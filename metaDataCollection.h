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
class metaDataCollection
{
private:
    trieTree m_dbs;
    trieTree m_charsetSizeList;
    std::string m_defaultCharset;
    sqlParser::sqlParser * m_SqlParser;
public:
    metaDataCollection();
    ~metaDataCollection();
    tableMeta * get(const char * database,const char * table,uint64_t fileID,uint64_t offset);
    int put(const char * database,const char * table,tableMeta * meta,uint64_t fileID,uint64_t offset);
    int purge(uint64_t fileID,uint64_t offset);
    int processDDL(const char * ddl,uint64_t fileID,uint64_t offset);
private:
    int createTable(sqlParser::handle * h,const sqlParser::newTableInfo *t,uint64_t fileID,uint64_t offset);
    int createTableLike(sqlParser::handle * h,const sqlParser::newTableInfo *t,uint64_t fileID,uint64_t offset);
    int alterTable(sqlParser::handle * h,const sqlParser::newTableInfo *t,uint64_t fileID,uint64_t offset);
    int processNewTable(sqlParser::handle * h,const sqlParser::newTableInfo *t,uint64_t fileID,uint64_t offset);
    int processOldTable(sqlParser::handle * h,const sqlParser::Table *table,uint64_t fileID,uint64_t offset);
    int processDatabase(const sqlParser::databaseInfo * database,uint64_t fileID,uint64_t offset);
public:
};
#endif /* METADATACOLLECTION_H_ */
