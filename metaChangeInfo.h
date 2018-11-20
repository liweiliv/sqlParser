/*
 * metaChangeInfo.h
 *
 *  Created on: 2018年11月20日
 *      Author: liwei
 */

#ifndef METACHANGEINFO_H_
#define METACHANGEINFO_H_
#include <string>
#include <list>
#include <stdint.h>
#include <stdlib.h>
typedef enum enum_field_types {
  MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY,
  MYSQL_TYPE_SHORT,  MYSQL_TYPE_LONG,
  MYSQL_TYPE_FLOAT,  MYSQL_TYPE_DOUBLE,
  MYSQL_TYPE_NULL,   MYSQL_TYPE_TIMESTAMP,
  MYSQL_TYPE_LONGLONG,MYSQL_TYPE_INT24,
  MYSQL_TYPE_DATE,   MYSQL_TYPE_TIME,
  MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
  MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
  MYSQL_TYPE_BIT,
  MYSQL_TYPE_TIMESTAMP2,
  MYSQL_TYPE_DATETIME2,
  MYSQL_TYPE_TIME2,
  MYSQL_TYPE_JSON=245,
  MYSQL_TYPE_NEWDECIMAL=246,
  MYSQL_TYPE_ENUM=247,
  MYSQL_TYPE_SET=248,
  MYSQL_TYPE_TINY_BLOB=249,
  MYSQL_TYPE_MEDIUM_BLOB=250,
  MYSQL_TYPE_LONG_BLOB=251,
  MYSQL_TYPE_BLOB=252,
  MYSQL_TYPE_VAR_STRING=253,
  MYSQL_TYPE_STRING=254,
  MYSQL_TYPE_GEOMETRY=255
} enum_field_types;
using namespace std;
namespace sqlParser
{
struct Table
{
    string database;
    string table;
    Table(){}
    Table(const Table&t):database(t.database),table(t.table){}
    Table(const string &db,const string &tab):database(db),table(tab){}
    Table &operator =(const Table &t)
    {database = t.database;
    table = t.table;
    return *this;}
};
class newColumnInfo
{
public:
    enum colummDDLType{
        COLUMN_IN_CREATE_TABLE,
        COLUMN_IN_ADD_COLUMN,
        COLUMN_IN_MODIFY_COLUMN,
        COLUMN_IN_CHANGE_COLUMN
    };
    string name;
    uint8_t type;
    bool after;
    int index;
    string afterColumnName;
    bool isString;
    string charset;
    uint32_t size;
    uint32_t precision;
    uint32_t decimals;
    list<string> setAndEnumValueList;
    bool m_signed;
    bool m_isPrimary;
    bool m_isUnique;
    bool m_generated;
    newColumnInfo():type(0),after(false),index(-1),isString(false),size(0),precision(0),
            decimals(0),m_signed(true),m_isPrimary(false),m_isUnique(false),m_generated(false)
    {

    }
};
class newKeyInfo
{
public:
    enum KeyType{
        PRIMARY_KEY,
        UNIQUE_KEY,
        FOREIGN_KEY,
        KEY,
        MAX_KEY_TYPE
    };
    string name;
    KeyType type;
    list<string> columns;
    string refrenceDatabase;
    string refrenceTable;
    newKeyInfo():type(MAX_KEY_TYPE)
    {

    }
};
class newTableInfo
{
public:
    enum tableChangeType{
        CREATE_TABLE,
        ALTER_TABLE,
        MAX_TABLE_TYPE
    };
    tableChangeType type;
    Table table;
    bool createLike;
    Table likedTable;
    string defaultCharset;
    list<newColumnInfo*> newColumns;
    list<string> oldColumns;
    list<newKeyInfo*> newKeys;
    list<string> oldKeys;
    newTableInfo():type(MAX_TABLE_TYPE),createLike(false)
    {
    }
    ~newTableInfo()
    {
        for(list<newColumnInfo*>::iterator iter = newColumns.begin();iter!=newColumns.end();iter++)
            delete *iter;
        for(list<newKeyInfo*>::iterator iter = newKeys.begin();iter!=newKeys.end();iter++)
            delete *iter;
    }
};
struct metaChangeInfo
{
public:
    string usedDatabase;
    list<newTableInfo*> newTables;
    list<Table> oldTables;
    ~metaChangeInfo()
    {
        for(list<newTableInfo*>::iterator iter = newTables.begin();iter!=newTables.end();iter++)
            delete *iter;
    }
};
enum parseValue
{
    OK, NOT_MATCH, INVALID, COMMENT, NOT_SUPPORT
};
struct handle;
struct statusInfo
{
    parseValue (*parserFunc)(handle* h,const string &sql);
    string sql;
    statusInfo * next;
    statusInfo():parserFunc(NULL),next(NULL){}
};
struct handle
{
    string dbName;
    metaChangeInfo meta;
    statusInfo * head;
    statusInfo * end;
    handle * next;
    handle() :
            head(NULL), end(NULL),next(NULL)
    {
    }
    ~handle()
    {
        if(head!=NULL)
        {
            while(head!=NULL)
            {
                statusInfo * tmp = head->next;
                delete head;
                if(head == end)
                    break;
                head = tmp;
            }
        }
        /*不递归调用next的析构函数，使用循环的方式，
         * 避免解析较大的测试文件形成了巨大的handle链表在析构时堆栈超过上限*/
        while(next != NULL)
        {
            handle * tmp =next->next;
            next->next = NULL;
            delete next ;
            next= tmp;
        }
    }
};
}
#endif /* METACHANGEINFO_H_ */
