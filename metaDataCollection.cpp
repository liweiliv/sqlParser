/*
 * metaDataCollection.cpp
 *
 *  Created on: 2018年11月5日
 *      Author: liwei
 */
#include <string.h>
#include "metaDataCollection.h"
#include "metaData.h"
#include "sqlParser.h"
#ifndef likely
# define likely(x)  __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
# define unlikely(x)    __builtin_expect(!!(x), 0)
#endif
using namespace sqlParser;
template<typename T>
class MetaTimeline
{
private:
    struct checkpoint
    {
        uint64_t fileID;
        uint64_t offset;
    };
    struct MetaInfo
    {
        T * meta;
        checkpoint begin;
        checkpoint end;
        MetaInfo * prev;
    };
    MetaInfo * m_current;
public:
    MetaTimeline(T * meta = NULL, uint64_t fileID = 0, uint64_t offset = 0) :
            m_current(NULL)
    {
        if (meta != NULL)
            put(meta, fileID, offset);
    }
    ~MetaTimeline()
    {
        purge(0xffffffffffffffffUL, 0xffffffffffffffffUL);
    }
    /*can be concurrent*/
    inline T * get(uint64_t fileID, uint64_t offset)
    {
        checkpoint c =
        { fileID, offset };
        MetaInfo * current = m_current, *first;
        __asm__ __volatile__("lfence" ::: "memory");
        int b = memcmp(&current->begin, &c, sizeof(checkpoint));
        if (likely(b <= 0))
        {
            int e = memcmp(&current->end, &c, sizeof(checkpoint));
            if (likely(e > 0))
                return current;
            else
            {
                first = current;
                __asm__ __volatile__("lfence" ::: "memory");
                MetaInfo * newer = m_current;
                int e = memcmp(&newer->end, &c, sizeof(checkpoint));
                if (e < 0)
                    return NULL;
                while (newer != current)
                {
                    b = memcmp(&newer->begin, &c, sizeof(checkpoint));
                    if (b < 0)
                        return newer->meta;
                    else
                        newer = newer->prev;
                }
                abort();
            }
        }
        else
        {
            MetaInfo * m = current->prev;
            while (m != NULL)
            {
                b = memcmp(&m->begin, &c, sizeof(checkpoint));
                if (b < 0)
                    return m;
                else
                    m = m->prev;
            }
            return NULL;
        }
    }
    /*must be serial*/
    int put(T * meta, uint64_t fileID, uint64_t offset)
    {
        MetaInfo * m = new MetaInfo;
        m->begin.fileID = fileID;
        m->begin.offset = offset;
        m->end.fileID = 0xffffffffffffffffUL;
        m->end.offset = 0xffffffffffffffffUL;
        m->meta = meta;
        if (m_current == NULL)
        {
            __asm__ __volatile__("sfence" ::: "memory");
            m_current = m;
            return 0;
        }
        else
        {
            if (0 >= memcmp(&m_current->begin, &m->begin, sizeof(checkpoint)))
            {
                delete m;
                return -1;
            }
            m->prev = m_current;
            m_current->end.fileID = fileID;
            m_current->end.offset = offset;
            __asm__ __volatile__("sfence" ::: "memory");
            m_current = m;
            return 0;
        }
    }
    int disableCurrent(uint64_t fileID, uint64_t offset)
    {
        return put(NULL, fileID, offset);
    }
    void purge(uint64_t fileID, uint64_t offset)
    {
        checkpoint c =
        { fileID, offset };
        MetaInfo * m = m_current;
        while (m != NULL)
        {
            if (0 < memcmp(&m->end, &c, sizeof(checkpoint)))
                m = m->prev;
            else
                break;
        }
        if (m == NULL)
            return;
        while (m != NULL)
        {
            MetaInfo * tmp = m->prev;
            if (m->meta != NULL)
                delete m->meta;
            delete m;
            m = tmp;
        }
    }
    static int destroy(void * m, int force)
    {
        delete static_cast<MetaTimeline*>(m);
        return 0;
    }
};
struct dbInfo
{
    trieTree  tables;
    std::string charset;
};
metaDataCollection::metaDataCollection() :
        m_dbs(MetaTimeline<trieTree>::destroy)
{
    m_SqlParser = new sqlParser::sqlParser();
    m_charsetSizeList.insertNCase((uint8_t*) "big5", (void*) (uint64_t) 2);
    m_charsetSizeList.insertNCase((uint8_t*) "dec8", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "cp850", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "hp8", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "koi8r", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "latin1", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "latin2", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "swe7", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "ascii", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "ujis", (void*) (uint64_t) 3);
    m_charsetSizeList.insertNCase((uint8_t*) "sjis", (void*) (uint64_t) 2);
    m_charsetSizeList.insertNCase((uint8_t*) "hebrew", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "tis620", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "euckr", (void*) (uint64_t) 2);
    m_charsetSizeList.insertNCase((uint8_t*) "koi8u", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "gb2312", (void*) (uint64_t) 2);
    m_charsetSizeList.insertNCase((uint8_t*) "greek", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "cp1250", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "gbk", (void*) (uint64_t) 2);
    m_charsetSizeList.insertNCase((uint8_t*) "latin5", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "armscii8", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "utf8", (void*) (uint64_t) 3);
    m_charsetSizeList.insertNCase((uint8_t*) "ucs2", (void*) (uint64_t) 2);
    m_charsetSizeList.insertNCase((uint8_t*) "cp866", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "keybcs2", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "macce", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "macroman", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "cp852", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "latin7", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "utf8mb4", (void*) (uint64_t) 4);
    m_charsetSizeList.insertNCase((uint8_t*) "cp1251", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "utf16", (void*) (uint64_t) 4);
    m_charsetSizeList.insertNCase((uint8_t*) "utf16le", (void*) (uint64_t) 4);
    m_charsetSizeList.insertNCase((uint8_t*) "cp1256", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "cp1257", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "utf32", (void*) (uint64_t) 4);
    m_charsetSizeList.insertNCase((uint8_t*) "binary", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "geostd8", (void*) (uint64_t) 1);
    m_charsetSizeList.insertNCase((uint8_t*) "cp932", (void*) (uint64_t) 2);
    m_charsetSizeList.insertNCase((uint8_t*) "eucjpms", (void*) (uint64_t) 3);
    m_charsetSizeList.insertNCase((uint8_t*) "gb18030", (void*) (uint64_t) 4);
}
metaDataCollection::~metaDataCollection()
{
    if (m_SqlParser != NULL)
        delete m_SqlParser;
}
tableMeta * metaDataCollection::get(const char * database, const char * table,
        uint64_t fileID, uint64_t offset)
{
    MetaTimeline<dbInfo> * db =
            static_cast<MetaTimeline<dbInfo>*>(m_dbs.findNCase(
                    (const unsigned char*) database));
    if (db == NULL)
        return NULL;
    dbInfo * currentDB = db->get(fileID, offset);
    if (currentDB == NULL)
        return NULL;
    MetaTimeline<tableMeta> * metas =
            static_cast<MetaTimeline<tableMeta>*>(currentDB->tables.findNCase(
                    (const unsigned char*) table));
    if (metas == NULL)
        return NULL;
    return metas->get(fileID, offset);
}
int metaDataCollection::put(const char * database, const char * table,
        tableMeta * meta, uint64_t fileID, uint64_t offset)
{
    MetaTimeline<dbInfo> * db =
            static_cast<MetaTimeline<dbInfo>*>(m_dbs.findNCase(
                    (const unsigned char*) database));
    bool newMeta = false;
    if (db == NULL)
        return -1;
    dbInfo * currentDB = db->get(fileID, offset);
    MetaTimeline<tableMeta> * metas =
            static_cast<MetaTimeline<tableMeta>*>(currentDB->tables.findNCase(
                    (const unsigned char*) table));
    if (metas == NULL)
    {
        newMeta = true;
        metas = new MetaTimeline<tableMeta>(meta, fileID, offset);
    }
    else
    {
        metas->put(meta, fileID, offset);
    }
    if (newMeta)
    {
        __asm__ __volatile__("mfence" ::: "memory");
        currentDB->tables.insertNCase((const unsigned char*) table, metas);
    }
    return 0;
}
static void copyColumn(columnMeta & column, const newColumnInfo* src)
{
    column.m_columnType = src->type;
    column.m_columnIndex = src->index;
    column.m_columnName = src->name;
    column.m_decimals = src->decimals;
    column.m_generated = src->generated;
    column.m_isPrimary = src->isPrimary;
    column.m_isUnique = src->isUnique;
    column.m_precision = src->precision;
    column.m_setAndEnumValueList.m_Count = 0;
    column.m_setAndEnumValueList.m_array = (char**) malloc(
            sizeof(char*) * src->setAndEnumValueList.size());
    for (list<string>::iterator iter = src->setAndEnumValueList.begin();
            iter != src->setAndEnumValueList.end(); iter++)
    {
        column.m_setAndEnumValueList.m_array[column.m_setAndEnumValueList.m_Count] =
                (char*) malloc((*iter).size() + 1);
        memcpy(
                column.m_setAndEnumValueList.m_array[column.m_setAndEnumValueList.m_Count],
                (*iter).c_str(), (*iter).size());
        column.m_setAndEnumValueList.m_array[column.m_setAndEnumValueList.m_Count][(*iter).size()] =
                '\0';
        column.m_setAndEnumValueList.m_Count++;
    }
    column.m_signed = src->isSigned;
    column.m_size = src->size;
}
int metaDataCollection::createTable(handle * h, const newTableInfo *t,
        uint64_t fileID, uint64_t offset)
{
    Table newTable = t->table;
    if (!h->dbName.empty())
        newTable.database = h->dbName;
    if (newTable.database.empty())
    {
        printf("no database\n");
        return -1;
    }
    databaseInfo * db = static_cast<databaseInfo*>(m_dbs.findNCase(
            (const unsigned char*) newTable.database.c_str()));
    if (db == NULL)
    {
        printf("unknown database :%s\n", newTable.database.c_str());
        return -1;
    }
    tableMeta * meta = new tableMeta;
    meta->m_columns = new columnMeta[t->newColumns.size()];
    meta->m_tableName = newTable.table;
    meta->m_charset = t->defaultCharset;
    if (meta->m_charset.empty())
        meta->m_charset = db->charset;

    for (list<newColumnInfo*>::const_iterator iter = t->newColumns.begin();
            iter != t->newColumns.end(); iter++)
    {
        newColumnInfo * c = *iter;
        columnMeta & column = meta->m_columns[meta->m_columnsCount];
        copyColumn(column, c);
        if (c->isString)
        {
            if (c->charset.empty())
                column.m_charset = meta->m_charset;
            void * _charsetSize = m_charsetSizeList.findNCase(
                    (uint8_t*) column.m_charset.c_str());
            if (_charsetSize == NULL)
            {
                printf("unknown charset %s\n", column.m_charset.c_str());
                delete meta;
                return -1;
            }
            column.m_size *= (uint64_t) _charsetSize;
        }
        meta->m_columnsCount++;
    }
    uint32_t ukCount = 0;
    for (list<newKeyInfo*>::const_iterator iter = t->newKeys.begin();
            iter != t->newKeys.end(); iter++)
        if ((*iter)->type == newKeyInfo::UNIQUE_KEY)
            ukCount++;
    if (ukCount > 0)
        meta->m_uniqueKeys = new stringArray[ukCount];
    for (list<newKeyInfo*>::const_iterator iter = t->newKeys.begin();
            iter != t->newKeys.end(); iter++)
    {
        newKeyInfo * k = *iter;
        if (k->type == newKeyInfo::PRIMARY_KEY)
        {
            meta->m_primaryKey = k->columns;
            for (int i = 0; i < meta->m_primaryKey.m_Count; i++)
            {
                columnMeta * _c = meta->getColumn(
                        meta->m_primaryKey.m_array[i]);
                if (_c == NULL)
                {
                    printf("can not find primary column %s in columns\n",
                            meta->m_primaryKey.m_array[i]);
                    delete meta;
                    return -1;
                }
                _c->m_isPrimary = true;
            }
        }
        else if (k->type == newKeyInfo::UNIQUE_KEY)
        {
            meta->m_uniqueKeys[meta->m_uniqueKeysCount].columns = k->columns;
            meta->m_uniqueKeys[meta->m_uniqueKeysCount].name = k->name;
            for (int i = 0;
                    i
                            < meta->m_uniqueKeys[meta->m_uniqueKeysCount].columns.m_Count;
                    i++)
            {
                columnMeta * _c =
                        meta->getColumn(
                                meta->m_uniqueKeys[meta->m_uniqueKeysCount].columns.m_array[i]);
                if (_c == NULL)
                {
                    printf("can not find unique column %s in columns\n",
                            meta->m_primaryKey.m_array[i]);
                    delete meta;
                    return -1;
                }
                _c->m_isUnique = true;
            }
            meta->m_uniqueKeysCount++;
        }
        else
            continue;
    }
    if (0
            != put(newTable.database.c_str(), newTable.table.c_str(), meta,
                    fileID, offset))
    {
        printf("insert new meta of table %s.%s failed",
                newTable.database.c_str(), newTable.table.c_str());
        delete meta;
        return -1;
    }
    return 0;
}
int metaDataCollection::createTableLike(handle * h, const newTableInfo *t,
        uint64_t fileID, uint64_t offset)
{
    Table newTable = t->table;
    if (!h->dbName.empty())
        newTable.database = h->dbName;
    if (newTable.database.empty())
    {
        printf("no database\n");
        return -1;
    }
    databaseInfo * db = static_cast<databaseInfo*>(m_dbs.findNCase(
            (const unsigned char*) newTable.database.c_str()));
    if (db == NULL)
    {
        printf("unknown database :%s\n", newTable.database.c_str());
        return -1;
    }
    Table likedTable = t->likedTable;
    if (!h->dbName.empty())
        likedTable.database = h->dbName;
    if (likedTable.database.empty())
    {
        printf("no database\n");
        return -1;
    }
    tableMeta * likedMeta = get(likedTable.database.c_str(),
            likedTable.table.c_str(), 0xffffffffffffffffUL,
            0xffffffffffffffffUL);
    if (likedMeta == NULL)
    {
        printf("create liked table %s.%s is not exist",
                likedTable.database.c_str(), likedTable.table.c_str())
        return -1;
    }
    tableMeta * meta = new tableMeta;
    *meta = *likedMeta;
    meta->m_tableName = newTable.table;
    if (0
            != put(newTable.database.c_str(), newTable.table.c_str(), meta,
                    fileID, offset))
    {
        printf("insert new meta of table %s.%s failed",
                newTable.database.c_str(), newTable.table.c_str());
        delete meta;
        return -1;
    }
    return 0;
}
int metaDataCollection::alterTable(handle * h, const newTableInfo *t,
        uint64_t fileID, uint64_t offset)
{
    Table newTable = t->table;
    if (!h->dbName.empty())
        newTable.database = h->dbName;
    if (newTable.database.empty())
    {
        printf("no database\n");
        return -1;
    }
    tableMeta * meta = get(newTable.database.c_str(), newTable.table.c_str(),
            0xffffffffffffffffUL, 0xffffffffffffffffUL);
    if (meta == NULL)
    {
        printf("unknown table %s.%s\n", newTable.database.c_str(),
                newTable.table.c_str());
        return -1;
    }
    tableMeta * newMeta = new tableMeta;
    *newMeta = *meta;
    /*update charset*/
    if (!t->defaultCharset.empty())
        newMeta->m_charset = t->defaultCharset;
    /*update new column*/
    for (list<newColumnInfo*>::const_iterator iter = t->newColumns.begin();
            iter != t->newColumns.end(); iter++)
    {
        newColumnInfo * c = *iter;
        columnMeta column;
        copyColumn(column, c);
        /*update default charset and string size*/
        if (c->isString)
        {
            if (c->charset.empty())
                column.m_charset = meta->m_charset;
            void * _charsetSize = m_charsetSizeList.findNCase(
                    (uint8_t*) column.m_charset.c_str());
            if (_charsetSize == NULL)
            {
                printf("unknown charset %s\n", column.m_charset.c_str());
                delete newMeta;
                return -1;
            }
            column.m_size *= (uint64_t) _charsetSize;
        }
        columnMeta * modifiedColumn = meta->getColumn(c->name.c_str());
        if (c->after)
        {
            /*不能alter table modify column_a after column_a，先执行drop是安全的*/
            if (modifiedColumn != NULL)
            {
                if (0 != newMeta->dropColumn(modifiedColumn->m_columnIndex))
                {
                    printf("drop column %s in %s.%s failed\n",
                            column.m_columnName.c_str(),
                            newTable.database.c_str(), newTable.table.c_str());
                    delete newMeta;
                    return -1;
                }
            }
            if (0 != newMeta->addColumn(&column, c->afterColumnName.c_str()))
            {
                printf("add column %s after %s in %s.%s failed\n",
                        column.m_columnName.c_str(), c->afterColumnName.c_str(),
                        newTable.database.c_str(), newTable.table.c_str());
                delete newMeta;
                return -1;
            }

        }
        else
        {
            if (modifiedColumn != NULL) //modify column,only update column
            {
                column.m_columnIndex = modifiedColumn->m_columnIndex;
                *modifiedColumn = column;
            }
            else
            {
                if (0 != newMeta->addColumn(&column))
                {
                    printf("add column %s in %s.%s failed\n",
                            column.m_columnName.c_str(),
                            newTable.database.c_str(), newTable.table.c_str());
                    delete newMeta;
                    return -1;
                }
            }
        }
    }
    /*drop old column*/
    for (list<string>::const_iterator iter = t->oldColumns.begin();
            iter != t->oldColumns.end(); iter++)
    {
        if (0 != newMeta->dropColumn((*iter).c_str()))
        {
            printf("alter table drop column %s ,but %s is not exist in %s.%s\n",
                    (*iter).c_str(), newTable.database.c_str(),
                    newTable.table.c_str());
            delete newMeta;
            return -1;
        }
    }
    /*update new key*/
    for (list<newKeyInfo*>::const_iterator iter = t->newKeys.begin();
            iter != t->newKeys.end(); iter++)
    {
        const newKeyInfo * key = *iter;
        if (key->type == newKeyInfo::PRIMARY_KEY)
        {
            if (newMeta->createPrimaryKey(key->columns) != 0)
            {
                printf("primary key is exits in %s.%s\n",
                        newTable.database.c_str(), newTable.table.c_str());
                delete newMeta;
                return -1;
            }
        }
        else if (key->type == newKeyInfo::UNIQUE_KEY)
        {
            if (newMeta->addUniqueKey(key->name.c_str(), key->columns) != 0)
            {
                printf("unique key %s is exits in %s.%s\n", key->name.c_str(),
                        newTable.database.c_str(), newTable.table.c_str());
                delete newMeta;
                return -1;
            }
        }
        else
            continue;
    }
    /*drop old key*/
    for (list<string>::const_iterator iter = t->oldKeys.begin();
            iter != t->oldKeys.end(); iter++)
    {
        if ((*iter) == "PRIMARY")
        {
            newMeta->dropPrimaryKey();
        }
        else
        {
            newMeta->dropUniqueKey((*iter).c_str());
        }
    }
    if (0
            != put(newTable.database.c_str(), newTable.table.c_str(), newMeta,
                    fileID, offset))
    {
        printf("insert new meta of table %s.%s failed",
                newTable.database.c_str(), newTable.table.c_str());
        delete meta;
        return -1;
    }
    return 0;
}

int metaDataCollection::processNewTable(handle * h, const newTableInfo *t,
        uint64_t fileID, uint64_t offset)
{
    if (t->type == newTableInfo::CREATE_TABLE)
    {
        if (t->createLike)
        {
            return createTableLike(h, t, fileID, offset);
        }
        else
        {
            return createTable(h, t, fileID, offset);
        }
    }
    else if (t->type == newTableInfo::ALTER_TABLE)
    {
        return alterTable(h, t, fileID, offset);
    }
    else
        return -1;
}
int metaDataCollection::processOldTable(handle * h, const Table *table,
        uint64_t fileID, uint64_t offset)
{
    MetaTimeline<dbInfo> * db = NULL;
    if (table->database.empty())
    {
        if (h->dbName.empty())
            return -1;
        db = static_cast<MetaTimeline<dbInfo>*>(m_dbs.findNCase(
                (const unsigned char*) h->dbName.c_str()));
    }
    else
        db = static_cast<MetaTimeline<dbInfo>*>(m_dbs.findNCase(
                (const unsigned char*) table->database.c_str()));
    if (db == NULL)
        return -1;
    dbInfo * currentDB = db->get(fileID, offset);
    if (currentDB == NULL)
        return -1;
    MetaTimeline<tableMeta> * metas =
            static_cast<MetaTimeline<tableMeta>*>(currentDB->tables.findNCase(
                    (const unsigned char*) table));
    if (metas == NULL)
        return -1;
    return metas->disableCurrent(fileID, offset);
}
int metaDataCollection::processDatabase(const databaseInfo * database,
        uint64_t fileID, uint64_t offset)
{
    MetaTimeline<dbInfo> * db =
            static_cast<MetaTimeline<dbInfo>*>(m_dbs.findNCase(
                    (const unsigned char*) database->name.c_str()));
    dbInfo * current = NULL;
    if (db == NULL)
    {
        if (database->type == databaseInfo::CREATE_DATABASE)
        {
            current = new dbInfo;
            current->charset = database->charset;
            if (current->charset.empty())
                current->charset = m_defaultCharset;
            db = new MetaTimeline<dbInfo>(current, fileID, offset);
            __asm__ __volatile__("mfence" ::: "memory");
            if (0
                    != m_dbs.insertNCase(
                            (const uint8_t *) database->name.c_str(), db))
            {
                delete db;
                return -1;
            }
            else
                return 0;
        }
        else
            return -1;
    }

    if (NULL == (current = db->get(fileID, offset)))
    {
        if (database->type == databaseInfo::CREATE_DATABASE)
        {
            current = new dbInfo;
            current->charset = database->charset;
            if (current->charset.empty())
                current->charset = m_defaultCharset;
            __asm__ __volatile__("mfence" ::: "memory");
            if (0 != db->put(current, fileID, offset))
            {
                delete current;
                return -1;
            }
            else
                return 0;
        }
        else
            return -1;
    }

    if (database->type == databaseInfo::CREATE_DATABASE)
        return -1;
    else if (database->type == databaseInfo::ALTER_DATABASE)
    {
        if (!database->charset.empty())
            current->charset = database->charset;
        return 0;
    }
    else if (database->type == databaseInfo::DROP_DATABASE)
    {
        return db->disableCurrent(fileID, offset);
    }
    else
        return -1;
}

int metaDataCollection::processDDL(const char * ddl, uint64_t fileID,
        uint64_t offset)
{
    handle * h = NULL;
    if (OK != m_SqlParser->parse(h, ddl))
    {
        printf("parse ddl %s failed\n", ddl);
        return -1;
    }
    handle * currentHandle = h;
    while (currentHandle != NULL)
    {
        if (currentHandle->meta.database.type
                != databaseInfo::MAX_DATABASEDDL_TYPE)
        {
            processDatabase(&currentHandle->meta.database, fileID, offset);
        }
        for (list<newTableInfo*>::const_iterator iter =
                currentHandle->meta.newTables.begin();
                iter != currentHandle->meta.newTables.end(); iter++)
        {
            processNewTable(currentHandle, *iter, fileID, offset);
        }
        for (list<Table>::const_iterator iter = currentHandle->meta.oldTables;
                iter != currentHandle->meta.oldTables.end(); iter++)
        {
            processOldTable(currentHandle, &(*iter), fileID, offset);
        }
    }
    return 0;
}
int metaDataCollection::purge(uint64_t fileID, uint64_t offset)
{
    for(trieTree::iterator iter = m_dbs.begin();iter.valid();iter.next())
    {
        MetaTimeline<dbInfo> * db = static_cast<MetaTimeline<dbInfo>*>(iter.value() );
        if(db == NULL)
            continue;
        db->purge(fileID,offset);
        dbInfo * dbMeta = db->get(0xffffffffffffffffUL,0xffffffffffffffffUL);
        if(dbMeta == NULL)
            continue;
        for(trieTree::iterator titer = dbMeta->tables.begin();titer.valid();titer.next())
        {
            MetaTimeline<tableMeta> * table = static_cast<MetaTimeline<tableMeta>*>(titer.value() );
            if(table == NULL)
                continue;
            table->purge(fileID,offset);
        }
    }
    return 0;
}

