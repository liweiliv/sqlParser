/*
 * sqlParser.cpp
 *
 *  Created on: 2018年11月10日
 *      Author: liwei
 */
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <list>
#include <map>
#include <string>
#include <assert.h>
#include "../metaData.h"
#include "binary_log_types.h"
#include "Log_r.h"
#include "stackLog.h"
#include "json.h"
using namespace std;
namespace sqlParser
{
enum statusNO
{
    start, create, createTable, createTableLike, newColumn,constraint
};
enum parseValue
{
    OK, NOT_MATCH, INVALID, COMMENT, NOT_SUPPORT
};
enum sqlType
{
    CREATE,
    DROP,
    ALTER,
    RENAME,
    CREATE_TABLE,
    DROP_TABLE,
    RENAME_TABLE,
    TRUNCATE_TABLE,
    ALTER_TABLE_NEW_COLUMN,
    ALTER_TABLE_MODIFY_COLUMN,
    ALTER_TABLE_ADD_KEY,
    CREATE_DATABASE,
    DROP_DATABASE,
    CREATE_TIRGGER,
    DROP_TRIGGER
};

class statusInfo
{
public:
    statusNO m_type;
    statusInfo * prev;
    statusInfo(statusNO type) :
            m_type(type), prev(NULL)
    {
    }
};
class CREATE_TABLE_Status: public statusInfo
{
    string dbName;
    string tableName;
public:
    CREATE_TABLE_Status(const char * db = NULL, const char * table = NULL) :
            statusInfo(createTable)
    {
        if (db != NULL)
            dbName = db;
        if (table != NULL)
            tableName = table;
    }
};
class CREATE_TABLE_LIKE_Status: public CREATE_TABLE_Status
{
public:
    string sourceDBName;
    string sourceTableName;
    CREATE_TABLE_LIKE_Status(const char *db = NULL, const char* table = NULL,
            const char * srcDB = NULL, const char *srcTable = NULL) :
            CREATE_TABLE_Status(db, table), statusInfo(createTableLike)
    {
        if (srcDB != NULL)
            sourceDBName = srcDB;
        if (srcTable != NULL)
            sourceTableName = srcTable;
    }
};
class NEW_COLUMN_Status: public statusInfo
{
public:
    columnMeta meta;
    NEW_COLUMN_Status() :
            statusInfo(newColumn)
    {
    }
};
class CONSTRAINT_Status: public statusInfo
{
public:
    enum constraintType
    {
        PRIMARY,UNIQUE,maxConsType
    };
    constraintType m_consType;
    char ** m_columnList;
    uint16_t m_columnListSize;
    string m_name;
    CONSTRAINT_Status() :
        statusInfo(constraint)
    {
        m_consType = maxConsType;
        m_columnList = NULL;
        m_columnListSize = 0;
    }
    ~CONSTRAINT_Status()
    {
        if(m_columnList!=NULL)
        {
            for(int idx = 0;idx<m_columnListSize;idx++)
            {
                if(m_columnList[idx]!=NULL)
                    free(m_columnList[idx]);
            }
            free(m_columnList);
        }
    }
};
struct handle
{
    string dbName;
    statusInfo * head;
    statusInfo * end;
};
struct status
{
    bool couldStart;
    bool couldEnd;
    uint32_t s;
    parseValue (*parse)(handle * h, const char *& sql);
    uint32_t nextCounts;
    uint32_t next[1];
};
status * allStatus;
uint32_t allStatusSize;
static bool isSpaceOrComment(const char *str)
{
    if ((*str == ' ' || *str == '\t' || *str == '\n'))
        return true;
    if (str[0] == '/' && str[1] == '*')
        return true;
    return false;
}
static const char * jumpOverSpace(const char * str)
{
    const char * p = str;
    while (*p == ' ' || *p == '\t' || *p == '\n')
        p++;
    return p;
}
static parseValue parse_Comment(handle *h, const char *& sql);
static const char * jumpOver(const char * str, const char * dest)
{
    const char * p = str;
    parse_Comment(NULL, p);
    p = jumpOverSpace(p);
    if (strncasecmp(str, dest, strlen(dest)) == 0)
        return str + strlen(dest);
    else
        return NULL;
}
static const char * endOfWord(const char * str)
{
    while (!isSpaceOrComment(str) && *str != '\0')
        str++;
    return str;
}
static const char * nextWord(const char * str)
{
    while (isSpaceOrComment(str))
    {
        parse_Comment(NULL, str);
        str = jumpOverSpace(str);
    }
    return str;
}
parseValue * parse(handle * h, const char * sql)
{
    statusInfo * start = new statusInfo;
    start->m_type = statusNO::start;
    start->prev = NULL;
    assert(h->head == NULL);
    h->head = h->end = start;
    while (true)
    {
        for (int i = 0; allStatus[i < h->end->m_type].nextCounts; i++)
        {
            parseValue rtv =
                    allStatus[allStatus[i < h->end->m_type].next[i]].parse(h,
                            sql);
            if (rtv == parseValue::OK || rtv == parseValue::COMMENT)
                break;
            else if (rtv == parseValue::NOT_MATCH || rtv == parseValue::COMMENT)
                continue;
            else if (rtv == parseValue::INVALID)
                return parseValue::INVALID;
            else
                abort();
        }
        sql = jumpOverSpace(sql);
        if (*sql == '\0')
        {
            if (allStatus[h->end->m_type].couldEnd)
                return parseValue::OK;
            else
                return parseValue::INVALID;
        }
    }
    return parseValue::OK;
}
static parseValue parse_Create(handle * h, const char *& sql)
{
    const char * p = jumpOverSpace(sql);
    if (strncasecmp(p, "create", 6) != 0 || !isSpaceOrComment(p + 6))
        return parseValue::NOT_MATCH;
    statusInfo * s = new statusInfo(CREATE);
    s->prev = h->end;
    h->end = s;
    sql = p + 6;
    return parseValue::OK;
}
static parseValue parse_Comment(handle *h, const char *& sql)
{
    const char * p = jumpOverSpace(sql);
    if (strncmp(p, "/*", 2) != 0)
        return parseValue::NOT_MATCH;
    p += 2;
    while (*p != '\0' && (*p != '*' || *(p - 1) == '\\' || *(p + 1) != '/'))
        p++;
    if (*p == '\0')
        return parseValue::NOT_MATCH;
    sql = p + 2;
    return parseValue::COMMENT;
}
static bool getName(const char * str, const char *& start, uint16_t &size,
        const char * &realEnd)
{
    char quote;
    if (*str == '`' || *str == '\'' || *str == '"')
    {
        quote = *str;
        start = str + 1;
        realEnd = strchr(start, quote);
        if (realEnd == NULL)
        {
            SET_STACE_LOG_AND_RETURN_(false,-1,"except [%c] in [%s]",quote,start);
        }
        size = realEnd - start;
        realEnd++;
        return true;
    }
    else
    {
        realEnd = start = str;
        while (!isSpaceOrComment(realEnd) && *realEnd != '.' && realEnd != '\0')
            realEnd++;
        if (realEnd == start)
        {
            SET_STACE_LOG_AND_RETURN_(false,-1,"name is empty @ [%s]",start);
        }
        size = realEnd - start;
        return true;
    }
}
/*
 * CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name (
 * */
static parseValue parse_CreateTable(handle *h, const char * sql)
{
    const char * p = jumpOverSpace(sql);
    if (h->end->m_type != statusNO::create)
        return parseValue::INVALID;
    const char * optional;
    if (NULL != (optional = jumpOver(p, "TEMPORARY")))
        p = optional;
    if (strncasecmp(p, "TABLE", 5) != 0 || !isSpaceOrComment(p + 6))
        return parseValue::NOT_MATCH;
    p = nextWord(p + 5);
    if (NULL != (optional = jumpOver(p, "IF")))
    {
        p = optional;
        if (NULL != (optional = jumpOver(p, "NOT")))
        {
            p = optional;
            if (NULL != (optional = jumpOver(p, "EXISTS")))
                p = optional;
            else
                return parseValue::NOT_MATCH;
        }
        else
            return parseValue::NOT_MATCH;
    }

    string db, table;
    const char * nameStart, *nameEnd;
    uint16_t nameSize;
    const char *name = nextWord(p);
    if (!getName(name, nameStart, nameSize, nameEnd))
        return parseValue::NOT_MATCH;
    if (nameEnd[0] == '.') //db.table
    {
        db.assign(nameStart, nameSize);
        if (!getName(nameEnd, nameStart, nameSize, nameEnd))
            return parseValue::NOT_MATCH;
        table.assign(nameStart, nameSize);
    }
    else //only table ,request db in handel
    {
        if (h->dbName.empty())
            return parseValue::INVALID;
        table.assign(nameStart, nameSize);
    }
    p = nextWord(nameEnd);
    if (*p == '(')
    {
        CREATE_TABLE_Status * s = new CREATE_TABLE_Status(CREATE_TABLE);
        s->dbName = db;
        s->tableName = table;
        s->prev = h->end;
        h->end = s;
        sql = p + 1;
        return parseValue::OK;
    }
    else if (strncasecmp(p, "LIKE", 4) == 0)
    {
        name = nextWord(p + 4);
        string srcDB, srcTable;
        if (!getName(name, nameStart, nameSize, nameEnd))
            return parseValue::NOT_MATCH;
        if (nameEnd[0] == '.') //db.table
        {
            srcDB.assign(nameStart, nameSize);
            if (!getName(nameEnd, nameStart, nameSize, nameEnd))
                return parseValue::NOT_MATCH;
            srcTable.assign(nameStart, nameSize);
        }
        else //only table ,request db in handel
        {
            srcDB = db.empty() ? h->dbName : db;
            srcTable.assign(nameStart, nameSize);
        }
        CREATE_TABLE_LIKE_Status * s = new CREATE_TABLE_LIKE_Status(
                CREATE_TABLE);
        s->dbName = db;
        s->tableName = table;
        s->sourceDBName = srcDB;
        s->sourceTableName = srcTable;
        s->prev = h->end;
        h->end = s;
        sql = p + 1;
        return parseValue::OK;
    }
    else if (strncasecmp(p, "AS", 2) == 0) //todo not support [create table as select] right now
        return parseValue::NOT_SUPPORT;
    else
        return parseValue::INVALID;
}
static bool getColumnType(NEW_COLUMN_Status *s, const char *&sql)
{
    /*BIT[(M)]*/
    if (strncasecmp(sql, "BIT", 3) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_BIT;
        const char * p = nextWord(sql + 3);
        if (*p == '(')
        {
            s->meta.m_size = 0;
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_size = s->meta.m_size * 10 + *p - '0';
                p++;
            }
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' @ [%s]", p);
            }
            sql = p + 1;
            return true;
        }
        else
        {
            s->meta.m_size = 1;
            sql = p;
            return true;
        }
    }
    /*TINYINT[(M)] [UNSIGNED] [ZEROFILL]*/
     if (strncasecmp(sql, "TINYINT", 7) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_TINY;
        const char * p = nextWord(sql + 7);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
                p++;
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*BOOLEAN*/
    if (strncasecmp(sql, "BOOLEAN", 7) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_TINY;
        s->meta.m_signed = true;
        sql += 7;
        return true;
    }
    /*BOOL*/
    if (strncasecmp(sql, "BOOL", 4) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_TINY;
        s->meta.m_signed = true;
        sql += 4;
        return true;
    }
    /*SMALLINT[(M)] [UNSIGNED] [ZEROFILL]*/
    if (strncasecmp(sql, "SMALLINT", 8) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_SHORT;
        const char * p = nextWord(sql + 8);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
                p++;
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]*/
    if (strncasecmp(sql, "MEDIUMINT", 9) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_INT24;
        const char * p = nextWord(sql + 9);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
                p++;
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*INTEGER[(M)] [UNSIGNED] [ZEROFILL]*/
    if (strncasecmp(sql, "INTEGER", 7) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_LONG;
        const char * p = nextWord(sql + 7);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
                p++;
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*INT[(M)] [UNSIGNED] [ZEROFILL]*/
     if (strncasecmp(sql, "INT", 3) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_LONG;
        const char * p = nextWord(sql + 3);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
                p++;
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*BIGINT[(M)] [UNSIGNED] [ZEROFILL]*/
     if (strncasecmp(sql, "BIGINT", 6) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_LONGLONG;
        const char * p = nextWord(sql + 3);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
                p++;
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]*/
     if (strncasecmp(sql, "DECIMAL", 7) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_NEWDECIMAL;//MYSQL_TYPE_DECIMAL has not used,so not support it
        const char * p = nextWord(sql + 7);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            s->meta.m_size = 0;
            s->meta.m_decimals = 0;//If D is omitted, the default is 0.
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_size = s->meta.m_size*10+p[0]-'0';
                p++;
            }
            p = nextWord(p);
            if (*p == ',')
            {
                p = nextWord(p+1);
                s->meta.m_decimals = 0;
                while (*p >= '0' && *p <= '9')
                {
                    s->meta.m_decimals = s->meta.m_decimals*10+p[0]-'0';
                    p++;
                }
                p = nextWord(p);
            }

            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        else
        {
            s->meta.m_size = 10;//If M is omitted, the default is 10
            s->meta.m_decimals = 0;
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]*/
     if (strncasecmp(sql, "FLOAT", 5) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_FLOAT;
        const char * p = nextWord(sql + 5);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            s->meta.m_size = 0; //actually we not care size...
            s->meta.m_decimals = 0;//If D is omitted, the default is 0.
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_size = s->meta.m_size*10+p[0]-'0';
                p++;
            }
            p = nextWord(p);
            if (*p == ',')
            {
                p = nextWord(p+1);
                s->meta.m_decimals = 0;
                while (*p >= '0' && *p <= '9')
                {
                    s->meta.m_decimals = s->meta.m_decimals*10+p[0]-'0';
                    p++;
                }
                p = nextWord(p);
            }

            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        else
        {
            s->meta.m_decimals = 31;
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]*/
     if (strncasecmp(sql, "DOUBLE", 6) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_DOUBLE;
        const char * p = nextWord(sql + 6);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            s->meta.m_size = 0; //actually we not care size...
            s->meta.m_decimals = 0;//If D is omitted, the default is 0.
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_size = s->meta.m_size*10+p[0]-'0';
                p++;
            }
            p = nextWord(p);
            if (*p == ',')
            {
                p = nextWord(p+1);
                s->meta.m_decimals = 0;
                while (*p >= '0' && *p <= '9')
                {
                    s->meta.m_decimals = s->meta.m_decimals*10+p[0]-'0';
                    p++;
                }
                p = nextWord(p);
            }

            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        else
        {
            s->meta.m_decimals = 31;
        }
        if (strncasecmp(p, "UNSIGNED", 8) == 0)
        {
            s->meta.m_signed = false;
            p += 8;
        }
        else
            s->meta.m_signed = true;
        p = nextWord(p);
        const char * end = jumpOver(p, "ZEROFILL");
        if (end)
            sql = end;
        else
            sql = p;
        return true;
    }
    /*DATETIME[(fsp)]*/
     if (strncasecmp(sql, "DATETIME", 8) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_DATETIME;
        s->meta.m_precision = 0;
        const char * p = nextWord(sql + 8);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            s->meta.m_columnType = MYSQL_TYPE_DATETIME2;//only MYSQL_TYPE_DATETIME2 has  precision
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_precision = s->meta.m_precision * 10 + p[0] - '0';
                p++;
            }
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        sql = p;
        return true;
    }
    /*DATE*/
     if (strncasecmp(sql, "DATE", 4) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_DATE;
        sql+=4;
        return true;
    }
    /*TIMESTAMP[(fsp)]*/
     if (strncasecmp(sql, "TIMESTAMP", 9) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_TIMESTAMP;
        s->meta.m_precision = 0;
        const char * p = nextWord(sql + 9);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            s->meta.m_columnType = MYSQL_TYPE_TIMESTAMP2;//only MYSQL_TYPE_TIMESTAMP2 has  precision
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_precision = s->meta.m_precision * 10 + p[0] - '0';
                p++;
            }
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        sql = p;
        return true;
    }
    /*TIME[(fsp)]*/
     if (strncasecmp(sql, "TIME", 4) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_TIME;
        s->meta.m_precision = 0;
        const char * p = nextWord(sql + 4);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            s->meta.m_columnType = MYSQL_TYPE_TIME2;//only MYSQL_TYPE_TIME2 has  precision
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_precision = s->meta.m_precision * 10 + p[0] - '0';
                p++;
            }
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        sql = p;
        return true;
    }
    /*YEAR[(4)]*/
     if (strncasecmp(sql, "YEAR", 4) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_YEAR;
        s->meta.m_precision = 0;
        const char * p = nextWord(sql + 4);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_precision = s->meta.m_precision * 10 + p[0] - '0';
                p++;
            }
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        else
            s->meta.m_precision = 4;
        sql = p;
        return true;
    }
    /*[NATIONAL] CHAR[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]*/
     if(strncasecmp(sql, "NATIONAL", 8) == 0)
     {
         const char * p = nextWord(sql + 8);
         if(strncasecmp(p, "CHAR", 4) != 0&&strncasecmp(p, "VARCHAR", 7)!=0)
         {
             SET_STACE_LOG_AND_RETURN_(false, -1, "except 'CHAR' or 'VARCHAR' after 'NATIONAL' @ [%s]", p);
         }
     }

     /*[NATIONAL] CHAR[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]*/
     if (strncasecmp(sql, "CHAR", 4) == 0)
    {
         s->meta.m_columnType = MYSQL_TYPE_STRING;
         s->meta.m_size = 0;
         const char * p = nextWord(sql + 4);
         if (*p == '(')
         {
             p = nextWord(sql + 1);
             while (*p >= '0' && *p <= '9')
             {
                 s->meta.m_size = s->meta.m_size * 10 + p[0] - '0';
                 p++;
             }
             p = nextWord(p);
             if (*p != ')')
             {
                 SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
             }
             p = nextWord(p + 1);
         }
         else
             s->meta.m_size = 1;
         if(strncasecmp(p,"CHARACTER",9)==0)
         {
             p = nextWord(p + 9);
             if(strncasecmp(p,"SET",3)==0)
             {
                 p = nextWord(p + 3);

             }
             else
             {
                 SET_STACE_LOG_AND_RETURN_(false, -1, "except 'SET' after 'CHARACTER' @ [%s]", p);
             }
             const char * end = endOfWord(p);
             s->meta.m_charset.assign(p,end-p);
             p = end;
         }
         if(strncasecmp(p,"COLLATE",7)==0)//ignore  COLLATE
         {
             p = nextWord(p + 7);
             p = endOfWord(p);
         }
         sql = p;
         return true;
    }
     /*[NATIONAL] VARCHAR(M) [CHARACTER SET charset_name] [COLLATE collation_name]*/
     if (strncasecmp(sql, "VARCHAR", 7) == 0)
    {
         s->meta.m_columnType = MYSQL_TYPE_VARCHAR;
         s->meta.m_size = 0;
         const char * p = nextWord(sql + 7);
         if (*p == '(')
         {
             p = nextWord(sql + 1);
             while (*p >= '0' && *p <= '9')
             {
                 s->meta.m_size = s->meta.m_size * 10 + p[0] - '0';
                 p++;
             }
             p = nextWord(p);
             if (*p != ')')
             {
                 SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
             }
             p = nextWord(p + 1);
         }
         else
             s->meta.m_size = 1;
         if(strncasecmp(p,"CHARACTER",9)==0)
         {
             p = nextWord(p + 9);
             if(strncasecmp(p,"SET",3)==0)
             {
                 p = nextWord(p + 3);

             }
             else
             {
                 SET_STACE_LOG_AND_RETURN_(false, -1, "except 'SET' after 'CHARACTER' @ [%s]", p);
             }
             const char * end = endOfWord(p);
             s->meta.m_charset.assign(p,end-p);
             p = end;
         }
         if(strncasecmp(p,"COLLATE",7)==0)//ignore  COLLATE
         {
             p = nextWord(p + 7);
             p = endOfWord(p);
         }
         sql = p;
         return true;
    }
     /*BINARY[(M)]*/
    if (strncasecmp(sql, "BINARY", 6) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_STRING;
        s->meta.m_size = 0;
        const char * p = nextWord(sql + 6);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_size = s->meta.m_size * 10 + p[0] - '0';
                p++;
            }
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        else
            s->meta.m_size = 1;
        sql = p;
        return true;
    }
    /*VARBINARY(M)*/
     if (strncasecmp(sql, "VARBINARY", 9) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_VAR_STRING;
        s->meta.m_size = 0;
        const char * p = nextWord(sql + 6);
        if (*p == '(')
        {
            p = nextWord(sql + 1);
            while (*p >= '0' && *p <= '9')
            {
                s->meta.m_size = s->meta.m_size * 10 + p[0] - '0';
                p++;
            }
            p = nextWord(p);
            if (*p != ')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' @ [%s]", p);
            }
            p = nextWord(p + 1);
        }
        else
            s->meta.m_size = 1;
        sql = p;
        return true;
    }
     /*TINYBLOB*/
     if (strncasecmp(sql, "TINYBLOB", 8) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_TINY_BLOB;
        sql+=8;
        return true;
    }
     /*TINYTEXT [CHARACTER SET charset_name] [COLLATE collation_name]*/
     if (strncasecmp(sql, "TINYTEXT", 8) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_TINY_BLOB;
        const char * p = nextWord(sql + 8);
        if(strncasecmp(p,"CHARACTER",9)==0)
        {
            p = nextWord(p + 9);
            if(strncasecmp(p,"SET",3)==0)
            {
                p = nextWord(p + 3);

            }
            else
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except 'SET' after 'CHARACTER' @ [%s]", p);
            }
            const char * end = endOfWord(p);
            s->meta.m_charset.assign(p,end-p);
            p = end;
        }
        if(strncasecmp(p,"COLLATE",7)==0)//ignore  COLLATE
        {
            p = nextWord(p + 7);
            p = endOfWord(p);
        }
        sql = p;
        return true;
    }
     /*BLOB[(M)]*/
     if (strncasecmp(sql, "BLOB", 4) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_BLOB;
        sql+=4;
        return true;
    }
     /*TEXT[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]*/
     if (strncasecmp(sql, "TEXT", 4) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_BLOB;
        const char * p = nextWord(sql + 4);
        if(strncasecmp(p,"CHARACTER",9)==0)
        {
            p = nextWord(p + 9);
            if(strncasecmp(p,"SET",3)==0)
            {
                p = nextWord(p + 3);

            }
            else
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except 'SET' after 'CHARACTER' @ [%s]", p);
            }
            const char * end = endOfWord(p);
            s->meta.m_charset.assign(p,end-p);
            p = end;
        }
        if(strncasecmp(p,"COLLATE",7)==0)//ignore  COLLATE
        {
            p = nextWord(p + 7);
            p = endOfWord(p);
        }
        sql = p;
        return true;
    }
     /*MEDIUMBLOB*/
     if (strncasecmp(sql, "MEDIUMBLOB", 10) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_MEDIUM_BLOB;
        sql+=10;
        return true;
    }
     /*MEDIUMTEXT [CHARACTER SET charset_name] [COLLATE collation_name]*/
    else if (strncasecmp(sql, "MEDIUMTEXT", 10) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_MEDIUM_BLOB;
        const char * p = nextWord(sql + 10);
        if(strncasecmp(p,"CHARACTER",9)==0)
        {
            p = nextWord(p + 9);
            if(strncasecmp(p,"SET",3)==0)
            {
                p = nextWord(p + 3);

            }
            else
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except 'SET' after 'CHARACTER' @ [%s]", p);
            }
            const char * end = endOfWord(p);
            s->meta.m_charset.assign(p,end-p);
            p = end;
        }
        if(strncasecmp(p,"COLLATE",7)==0)//ignore  COLLATE
        {
            p = nextWord(p + 7);
            p = endOfWord(p);
        }
        sql = p;
        return true;
    }
    else if (strncasecmp(sql, "LONGBLOB", 8) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_LONG_BLOB;
        sql+=8;
        return true;
    }
    else if (strncasecmp(sql, "LONGTEXT", 8) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_LONG_BLOB;
        const char * p = nextWord(sql + 8);
        if(strncasecmp(p,"CHARACTER",9)==0)
        {
            p = nextWord(p + 9);
            if(strncasecmp(p,"SET",3)==0)
            {
                p = nextWord(p + 3);

            }
            else
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except 'SET' after 'CHARACTER' @ [%s]", p);
            }
            const char * end = endOfWord(p);
            s->meta.m_charset.assign(p,end-p);
            p = end;
        }
        if(strncasecmp(p,"COLLATE",7)==0)//ignore  COLLATE
        {
            p = nextWord(p + 7);
            p = endOfWord(p);
        }
        sql = p;
        return true;
    }
     /*ENUM('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]*/
    if (strncasecmp(sql, "ENUM", 4) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_ENUM;
        const char * p = nextWord(sql + 4);
        if(*p!='(')
        {
            SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' after 'ENUM' @ [%s]", p);
        }
        p = nextWord(p + 1);
        s->meta.m_setAndEnumValueListSize = 0;
        const char * nameStart, *nameEnd,*tmp = p;
        uint16_t nameSize = 0;
        while(*p!=')'&&*p!=')')
        {
            if(!getName(p,nameStart,nameSize,nameEnd))
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except {'value'} after 'ENUM(' @ [%s]", p);
            }
            s->meta.m_setAndEnumValueListSize++;
            p=nextWord(p);
            if(*p==',')
                p = nextWord(p+1);
            else if(*p!=')')
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except {'value'} after 'ENUM(' @ [%s]", p);
            }
        }
        s->meta.m_setAndEnumValueList = (char**)malloc(sizeof(char*)*s->meta.m_setAndEnumValueListSize);
        memset(s->meta.m_setAndEnumValueList,0,sizeof(char*)*s->meta.m_setAndEnumValueListSize);
        p = tmp;
        for(int idx = 0;*p!=')'&&*p!=')';)
        {
            getName(p,nameStart,nameSize,nameEnd);
            s->meta.m_setAndEnumValueList[idx] = (char*)malloc(nameSize+1);
            memcpy(s->meta.m_setAndEnumValueList[idx],nameStart,nameSize);
            s->meta.m_setAndEnumValueList[idx][nameSize] = '\0';
            p=nextWord(p);
            if(*p==',')
                p = nextWord(p+1);
        }
        if(*p!=')')
        {
            SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' after enum values @ [%s]", p);
        }
        p = nextWord(p+1);
        if(strncasecmp(p,"CHARACTER",9)==0)
        {
            p = nextWord(p + 9);
            if(strncasecmp(p,"SET",3)==0)
            {
                p = nextWord(p + 3);

            }
            else
            {
                SET_STACE_LOG_AND_RETURN_(false, -1, "except 'SET' after 'CHARACTER' @ [%s]", p);
            }
            const char * end = endOfWord(p);
            s->meta.m_charset.assign(p,end-p);
            p = end;
        }
        if(strncasecmp(p,"COLLATE",7)==0)//ignore  COLLATE
        {
            p = nextWord(p + 7);
            p = endOfWord(p);
        }
        sql = p;
        return true;
    }
    /*SET('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]*/
     if (strncasecmp(sql, "SET", 3) == 0)
         {
         s->meta.m_columnType = MYSQL_TYPE_SET;
         const char * p = nextWord(sql + 3);
         if(*p!='(')
         {
             SET_STACE_LOG_AND_RETURN_(false, -1, "except '(' after 'ENUM' @ [%s]", p);
         }
         p = nextWord(p + 1);
         s->meta.m_setAndEnumValueListSize = 0;
         const char * nameStart, *nameEnd,*tmp = p;
         uint16_t nameSize = 0;
         while(*p!=')'&&*p!=')')
         {
             if(!getName(p,nameStart,nameSize,nameEnd))
             {
                 SET_STACE_LOG_AND_RETURN_(false, -1, "except {'value'} after 'ENUM(' @ [%s]", p);
             }
             s->meta.m_setAndEnumValueListSize++;
             p=nextWord(p);
             if(*p==',')
                 p = nextWord(p+1);
             else if(*p!=')')
             {
                 SET_STACE_LOG_AND_RETURN_(false, -1, "except {'value'} after 'ENUM(' @ [%s]", p);
             }
         }
         s->meta.m_setAndEnumValueList = (char**)malloc(sizeof(char*)*s->meta.m_setAndEnumValueListSize);
         memset(s->meta.m_setAndEnumValueList,0,sizeof(char*)*s->meta.m_setAndEnumValueListSize);
         p = tmp;
         for(int idx = 0;*p!=')'&&*p!=')';)
         {
             getName(p,nameStart,nameSize,nameEnd);
             s->meta.m_setAndEnumValueList[idx] = (char*)malloc(nameSize+1);
             memcpy(s->meta.m_setAndEnumValueList[idx],nameStart,nameSize);
             s->meta.m_setAndEnumValueList[idx][nameSize] = '\0';
             p=nextWord(p);
             if(*p==',')
                 p = nextWord(p+1);
         }
         if(*p!=')')
         {
             SET_STACE_LOG_AND_RETURN_(false, -1, "except ')' after enum values @ [%s]", p);
         }
         p = nextWord(p+1);
         if(strncasecmp(p,"CHARACTER",9)==0)
         {
             p = nextWord(p + 9);
             if(strncasecmp(p,"SET",3)==0)
             {
                 p = nextWord(p + 3);

             }
             else
             {
                 SET_STACE_LOG_AND_RETURN_(false, -1, "except 'SET' after 'CHARACTER' @ [%s]", p);
             }
             const char * end = endOfWord(p);
             s->meta.m_charset.assign(p,end-p);
             p = end;
         }
         if(strncasecmp(p,"COLLATE",7)==0)//ignore  COLLATE
         {
             p = nextWord(p + 7);
             p = endOfWord(p);
         }
         sql = p;
         return true;
         }
    else if (strncasecmp(sql, "GEOMETRY", 8) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_GEOMETRY;
        sql+=8;
        return true;
    }
    else if (strncasecmp(sql, "JSON", 4) == 0)
    {
        s->meta.m_columnType = MYSQL_TYPE_JSON;
        s->meta.m_charset = "utf8mb4";
        sql+=4;
        return true;
    }
    else
    {
        SET_STACE_LOG_AND_RETURN_(false, -1, "unspport type @ [%s]", sql);
        return false;
    }
}

class SQLWord
{
public:
    bool m_optional;
    bool m_included;
    uint32_t m_id;
    enum SQLWordType
    {
        SQL_ARRAY,
        SQL_SIGNLE_WORD
    };
    SQLWordType m_type;
    virtual parseValue match(handle * h, const char *& sql) = 0;
    virtual SQLWord(uint32_t id,SQLWordType t,bool optional = false):m_id(id),m_type(t),m_optional(optional),m_included(false)
    {}
};
class SQLSingleWord :public SQLWord
{
public:
    enum sqlSingleWordType
    {
        CHAR,
        NAME,//dbname,tablename,column name
        ARRAY,//"xxxx" or 'xxxx'
        STRING,
        ANY_STRING,
        BRACKETS
    };
    static const char * KeyWord[] = {"(","PRIMARY","UNIQUE","KEY","AS","FOREIGN","USING"};
    string m_word;
    sqlSingleWordType m_wtype;
    parseValue (*m_parser)(handle * h, string sql);
    SQLSingleWord(uint32_t id,bool optional,sqlSingleWordType type,string word):SQLWord(id,SQL_SIGNLE_WORD,optional),m_word(word),m_wtype(type),
            m_parser(NULL)
    {
    }
    SQLSingleWord(const SQLSingleWord &s):SQLWord(s.m_id,SQL_SIGNLE_WORD,s.m_optional),m_word(s.m_word),
            m_wtype(s.m_type),
            m_parser(s.m_parser)
    {
    }
    virtual parseValue match(handle *h,const char *& sql)
    {
        const char * p = nextWord(sql);
        parseValue rtv = parseValue::OK;
        switch(m_type)
        {
        case BRACKETS:
        {
            if(*p!='(')
                return parseValue::NOT_MATCH;
            const char * end = p;
            int32_t bracketCount = 1;
            while(*end!='\0')
            {
                if(*end=='(')
                    bracketCount ++;
                else if(*end==')')
                {
                    if(--bracketCount<=0)
                        break;
                }
                end++;
            }
            if(*end=='\0')
                return parseValue::NOT_MATCH;
            if(m_parser!=NULL)
                rtv = m_parser(p,string(p,end-p));
            sql = end+1;
            break;
        }
        case CHAR:
        {
            char c = *p;
            if(c>='A'||c<='Z')
                c+='a'-'A';
            if(m_word[0]!=c)
                return parseValue::NOT_MATCH;
            if(m_parser!=NULL)
                rtv = m_parser(h,string(p,1));
            sql=p+1;
            break;
        }
        case ANY_STRING:
        {
            const char * end = endOfWord(p);
            if(end == NULL)
                return parseValue::NOT_MATCH;
            for(uint32_t idx = 0;idx<sizeof(KeyWord)/sizeof(const char*);idx++)
            {
                if(strlen(KeyWord[idx])==end-p&&strcasecmp(p,KeyWord[idx])==0)
                    return parseValue::NOT_MATCH;
            }
            if(m_parser!=NULL)
                rtv = m_parser(h,string(p,end-p));
            if(rtv!=parseValue::OK)
                return rtv;
            sql = end;
            break;
        }
        case NAME:
        {
            const char * nameStart, *nameEnd;
            uint16_t nameSize;
            if(!getName(p, nameStart, nameSize, nameEnd))
                return parseValue::NOT_MATCH;
            if(*p!='\''&&*p!='`'&&*p!='"')
            {
                for(uint32_t idx = 0;idx<sizeof(KeyWord)/sizeof(const char*);idx++)
                {
                    if(strlen(KeyWord[idx])==nameSize&&strcasecmp(nameStart,KeyWord[idx])==0)
                        return parseValue::NOT_MATCH;
                }
            }
            if(m_parser!=NULL)
                rtv = m_parser(h,string(nameStart,nameSize));
            if(rtv!=parseValue::OK)
                return rtv;
            sql = nameEnd;
            break;
        }
        case STRING:
        {
            if(strncasecmp(m_word.c_str(),p,m_word.size())!=0||!isSpaceOrComment(p+m_word.size()))
                return parseValue::NOT_MATCH;
            if(m_parser!=NULL)
                rtv = m_parser(h,m_word);
            if(rtv!=parseValue::OK)
                return rtv;
            sql = p+m_word.size();
            break;
        }
        case ARRAY:
        {
            if(*p!='\''||*p!='"')
                return parseValue::NOT_MATCH;
            char quote = *p;
            const char * end = p+1;
            while(*end!='\0'&&(*end!=quote||*(end-1)=='\\'))
                end++;
            if (*end == '\0')
                return parseValue::NOT_MATCH;
            if (m_parser != NULL)
                rtv = m_parser(h, string(p + 1, end - p - 1));
            if (rtv != parseValue::OK)
                return rtv;
            sql = end+1;
            break;
        }
        default:
            return parseValue::NOT_SUPPORT;
        }
        return rtv;
    }
};
class SQLWordArray :public SQLWord
{
public:
    list<SQLWord *> m_words;
    bool m_or;
    bool m_loop;
    SQLWordArray(uint32_t id,bool optional,bool _or,bool loop):SQLWord(id,SQL_ARRAY,optional),m_or(_or),m_loop(loop)
    {
    }
    ~SQLWordArray()
    {
        for(list<SQLWord *>::iterator iter = m_words.begin();iter!=m_words.end();iter++)
        {
            SQLWord * s = static_cast<SQLWord*>(*iter);
            if(s!=NULL)
            {
                if(!s->m_included)
                    delete (*iter);
            }
        }
    }
    SQLWordArray(const SQLWordArray&s):SQLWord(s.m_id,SQL_ARRAY,s.m_optional),m_or(s.m_or),m_loop(s.m_loop)
    {

    }
    void append(SQLWord *s)
    {
        m_words.push_back(s);
    }
    virtual parseValue match(handle *h,const char *& sql)
    {
        parseValue rtv = parseValue::OK;
        bool matched = false;;
        statusInfo * top = h->end;
        for(list<SQLWord *>::iterator iter = m_words.begin();iter!=m_words.end();)
        {
            SQLWord * s = *iter;
            const char * str =sql;
            if((s)==NULL)
            {
                iter++;
                continue;
            }
            if(s->m_type==SQLWord::SQLWordType::SQL_ARRAY)
            {
                rtv = static_cast<SQLWordArray*>(s)->match(h,str);
            }
            else if(s->m_type==SQLWord::SQLWordType::SQL_SIGNLE_WORD)
            {
                rtv = static_cast<SQLSingleWord*>(s)->match(h,str);
            }
            if(rtv!=parseValue::OK)
            {
                if(m_or||s->m_optional)
                {
                    iter++;
                    continue;
                }
                else if(m_loop&&matched)
                    return parseValue::OK;
                else
                    break;
            }
            else
            {
                sql = str;
                if(m_loop)
                {
                    iter = m_words.begin();
                    continue;
                }
                return parseValue::OK;
            }
        }
        if(rtv!=parseValue::OK)
        {
            for(statusInfo * s = h->end;s!=top;)
            {
                statusInfo * tmp = s->prev;
                delete s;
                s = tmp;
            }
            h->end = top;
        }
        return rtv;
    }
};
class sqlParser
{
private:
    SQLWord * loadSQlWordFromJson(jsonValue *json)
    {
        SQLWord * s = NULL;
        jsonObject * obj = static_cast<jsonObject*>(json);
        jsonValue * value = obj->get("OPTIONAL");
        bool optional = false; //default  false
        if (value != NULL)
        {
            if (value->t != jsonObject::BOOL)
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect bool type");
            }
            optional = static_cast<jsonBool*>(value)->m_value;
        }
        bool OR = false;
        value = obj->get("OR");
        if (value != NULL)
        {
            if (value->t != jsonObject::BOOL)
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect bool type");
            }
            OR = static_cast<jsonBool*>(value)->m_value;
        }
        bool loop = false;
        value = obj->get("LOOP");
        if (value != NULL)
        {
            if (value->t != jsonObject::BOOL)
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect bool type");
            }
            loop = static_cast<jsonBool*>(value)->m_value;
        }
        int id = 0;
        if ((value = obj->get("ID")) != NULL)
        {
            if (value->t != jsonObject::NUM)
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect num");
            }
            id = static_cast<jsonNum*>(value)->m_value;
        }
        else if(NULL!=(value = obj->get("INCLUDE")))
        {
            if (value->t != jsonObject::NUM)
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect num");
            }
            map<uint32_t,SQLWord *>::iterator iter = m_parseTree.find(static_cast<jsonNum*>(value)->m_value);
            if(iter == m_parseTree.end())
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "can not find include id %d in parse tree",static_cast<jsonNum*>(value)->m_value);
            }
            iter->second->m_included = true;//it will not be free by parent when destroy
            return iter->second;
        }
        else
        {
            SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect ID");
        }

        if ((value = obj->get("WORD")) != NULL)
        {
            if (value->t != jsonObject::STRING)
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect STRING type");
            }
            if (static_cast<jsonString*>(value)->m_value == "_A_")
                s = new SQLSingleWord(id, optional, SQLSingleWord::ARRAY, "");
            else if (static_cast<jsonString*>(value)->m_value == "_AS_")
                s = new SQLSingleWord(id, optional, SQLSingleWord::ANY_STRING,
                        "");
            else if (static_cast<jsonString*>(value)->m_value == "_N_")
                s = new SQLSingleWord(id, optional, SQLSingleWord::NAME, "");
            else if (static_cast<jsonString*>(value)->m_value == "_B_")
                s = new SQLSingleWord(id, optional, SQLSingleWord::BRACKETS, "");
            else if (strncmp(static_cast<jsonString*>(value)->m_value.c_str(),
                    "_S_:", 4) == 0)
                s = new SQLSingleWord(id, optional, SQLSingleWord::STRING,
                        static_cast<jsonString*>(value)->m_value.c_str() + 4);
            else if (strncmp(static_cast<jsonString*>(value)->m_value.c_str(),
                    "_C_:", 4) == 0)
                s = new SQLSingleWord(id, optional, SQLSingleWord::CHAR,
                        static_cast<jsonString*>(value)->m_value.c_str() + 4);
            else
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect STRING type");
            }
            return s;
        }
        else if ((value = obj->get("VALUE")) != NULL)
        {
            if (value->t != jsonObject::ARRAY)
            {
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect ARRAY type");
            }
            s = new SQLWordArray(id, optional, OR,loop);
            for (list<jsonValue*>::iterator iter =
                    static_cast<jsonArray*>(value)->m_values.begin();
                    iter != static_cast<jsonArray*>(value)->m_values.end();
                    iter++)
            {
                if ((*iter)->t != jsonObject::OBJECT)
                {
                    delete s;
                    SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect OBJECT type");
                }
                SQLWord * child = loadSQlWordFromJson(*iter);
                if (child == NULL)
                {
                    delete s;
                    SET_STACE_LOG_AND_RETURN_(NULL, -1,
                            "parse child parse tree failed");
                }
                static_cast<SQLWordArray*>(s)->append(child);
            }
        }
        return s;
    }
private:
    map<uint32_t,SQLWord *> m_parseTree;
public:
    int LoadGram(const char *config)
    {
        int32_t size;
        jsonValue * v = NULL;
        const char * p = nextWord(config);
        while(NULL!=(v = jsonValue::Parse(p, size)))
        {
            SQLWord * s = loadSQlWordFromJson(v);
            if(s == NULL)
            {
                SET_STACE_LOG_AND_RETURN_(-1, -1,
                        "load  parse tree failed");
            }
            m_parseTree.insert(pair<uint32_t,SQLWord>(s->m_id,s));
            p = nextWord(p+size);
            if(p[0]=='\0')
                break;
        }
        return 0;
    }
    parseValue parse(handle *h,const char * sql)
    {
        for(map<uint32_t,SQLWord *>::iterator iter = m_parseTree.begin();iter!=m_parseTree.end();iter++)
        {
            SQLWord * s = static_cast<SQLWord*>(iter->second);
            if(s->match(h,sql)!=parseValue::OK)
                continue;
            else
                return parseValue::OK;
        }
        return parseValue::NOT_MATCH;
    }
}
parseValue match(const char * sql)
{

}
/*
column_definition:
    data_type [NOT NULL | NULL] [DEFAULT {literal | (expr)} ]
      [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY]
      [COMMENT 'string']
      [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
      [STORAGE {DISK|MEMORY|DEFAULT}]
      [reference_definition]
  | data_type [GENERATED ALWAYS] AS (expression)
      [VIRTUAL | STORED] [NOT NULL | NULL]
      [UNIQUE [KEY]] [[PRIMARY] KEY]
      [COMMENT 'string']
    */
static parseValue parse_NewColumn(handle *h, const char * sql)
{
    const char * p = nextWord(sql);
    const char * nameStart, *nameEnd;
    uint16_t nameSize;
    if (!getName(p, nameStart, nameSize, nameEnd))
        return parseValue::NOT_MATCH;
    NEW_COLUMN_Status *s = new NEW_COLUMN_Status();
    /*get column name*/
    s->meta.m_columnName.assign(nameStart, nameSize);
    /*get column_definition*/
    p = nextWord(nameEnd);
    if(!getColumnType(s,p))
    {
        delete s;
        SET_STACE_LOG_AND_RETURN_(parseValue::NOT_MATCH, -1, "get column type failed @ %s", sql);
    }
    p = nextWord(p);
    while(*p!=','&&*p!=')')
    {
        if(strncasecmp(p,"COMMENT",7)==0)
        {
            p = nextWord(p);
            if(*p!='\''||*p!='"')
            {
                delete s;
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "COMMENT must begin with ['] or  [\"] @%s ", sql);
            }
            char quote = *p;
            const char * end = strchr(p+1,quote);
            if(end == NULL)
            {
                delete s;
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "COMMENT must end with ['] or  [\"] @%s ", sql);
            }
            p = end+1;
            continue;
        }
        /*[ GENERATED ALWAYS ] AS ( <expression> ) [ VIRTUAL|STORED ]*/
        if(strncasecmp(p,"GENERATED",9)==0||strncasecmp(p,"AS",2)==0)//virtual column
        {
            const char * end = strchr(p,')');
            if(end==NULL)
            {
                delete s;
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "expect ')' in  virtual column @%s ", sql);
            }
            p = nextWord(end+1);
            if(strncasecmp(p,"STORED",6)==0)//todo ,not use right now
            {

            }
            else //default is VIRTUAL
            {

            }
            s->meta.m_generated = true;
        }
        if(strncasecmp(p,"PRIMARY",7)==0)
        {
            p = nextWord(p+7);
            if(strncasecmp(p,"KEY",3)!=0)
            {
                s->meta.m_isPrimary = true;
                p+=3;
                continue;
            }
            else
            {
                delete s;
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "expect 'KEY' after 'PRIMARY' @%s ", sql);
            }
        }
        if(strncasecmp(p,"UNIQUE",6)==0)
        {
            p = nextWord(p+6);
            if(strncasecmp(p,"KEY",3)!=0)
            {
                s->meta.m_isUnique = true;
                p+=3;
                continue;
            }
            else
            {
                delete s;
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "expect 'KEY' after 'UNIQUE' @%s ", sql);
            }
        }
        const char *next = endOfWord(p);
        if(next==NULL)
        {
            delete s;
            SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "unexpected end of sql ", sql);
        }
        p = nextWord(next);
    }
    if(*p==',')
        p++;
    sql = p;
    s->prev = h->end;
    h->end = s;
    return parseValue::OK;
}
static parseValue parse_FOREIGN_KEY(handle *h, const char * sql)
{

}
/**
 *   | [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (key_part,...)
      [index_option] ...
  | [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY]
      [index_name] [index_type] (key_part,...)
      [index_option] ...
  | [CONSTRAINT [symbol]] FOREIGN KEY
      [index_name] (col_name,...) reference_definition
 */
static parseValue parse_CONSTRAINT(handle *h, const char * sql)
{
    const char * p = nextWord(sql);
    CONSTRAINT_Status * s = NULL;
    string name;
    if(strncasecmp(p,"CONSTRAINT",10)==0)
    {
        p = nextWord(p+10);
        if(strncasecmp(p,"PRIMARY",7)!=0&&strncasecmp(p,"UNIQUE",6)!=0&&strncasecmp(p,"FOREIGN",7)!=0)
        {
            const char * end = endOfWord(p);
            if(end == NULL)
            {
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "unexpected end of sql ", sql);
            }
            name.assign(p,end-p);
            p = nextWord(end);//jump over [symbol]
        }
    }
    if(strncasecmp(p,"PRIMARY",7)==0)
    {
        p = nextWord(p+7);
        if(strncasecmp(p,"KEY",3)!=0)
        {
            SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "expect 'KEY' after 'PRIMARY' @%s ", sql);
        }
        p = nextWord(p+3);
        if(*p!='(')
        {
            if(strncasecmp(p,"USING",5)!=0)
            {
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "expect 'USING' after 'PRIMARY KEY' @%s ", sql);
            }
            const char * end = endOfWord(nextWord(p+5));
            if(end == NULL)
            {
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "unexpected end of sql ", sql);
            }
            p = nextWord(end);//jump over [index_type]
        }
        if(*p!='(')
        {
            SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "expect '(' after 'PRIMARY KEY' @%s ", sql);
        }
        s = new CONSTRAINT_Status();
        s->m_name = name;
        s->m_consType = CONSTRAINT_Status::constraintType::PRIMARY;
    }
    else if(strncasecmp(p,"UNIQUE",7)==0)
    {
        p = nextWord(p+7);
        if(strncasecmp(p,"KEY",3)!=0)
        {
            if(strncasecmp(p,"INDEX",5)!=0)
            {
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "expect 'KEY' OR 'INDEX' after 'UNIQUE' @%s ", sql);
            }
            else
                p = nextWord(p+5);
        }
        else
            p = nextWord(p+3);
        const char * indexName = NULL,*end = NULL;
FIND_NEXT_UK_WORD:
        if(strncasecmp(p,"USING",5)==0)
        {
            end = endOfWord(nextWord(p+5));
            if(end == NULL)
            {
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "unexpected end of sql:%s ", sql);
            }
            p = nextWord(end);//jump over [index_type]
        }
        if(*p!='(')
        {
            if(indexName!=NULL)
            {
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "unexpected error in sql:%s ", sql);
            }
            indexName = p;
            end = endOfWord(p);
            if(end == NULL)
            {
                SET_STACE_LOG_AND_RETURN_(parseValue::INVALID, -1, "unexpected end of sql:%s ", sql);
            }
            p = end;
        }
        if(*p!='(')
            goto FIND_NEXT_UK_WORD;
        s = new CONSTRAINT_Status();
        s->m_consType = CONSTRAINT_Status::constraintType::UNIQUE;
        if(name.empty())
            s->m_name.assign(p,endOfWord(p)-p);
        else
            s->m_name = name;
    }
    else if(strncasecmp(p,"FOREIGN",7)==0)
    {
        return parse_FOREIGN_KEY(h,sql);
    }
}
}

