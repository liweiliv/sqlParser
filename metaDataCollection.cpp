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
    struct MetaInfo
    {
        T * meta;
        uint64_t begin;
        uint64_t end;
        MetaInfo * prev;
    };
    MetaInfo * m_current;
    uint64_t m_id;
    uint16_t m_version;
public:
    MetaTimeline(uint64_t id,T * meta = NULL, uint64_t fileID = 0, uint64_t offset = 0) :
            m_current(NULL),m_id(id),m_version(0)
    {
        if (meta != NULL)
        {
            put(meta, fileID, offset);
            meta->id = m_id|m_version;
        }
    }
    ~MetaTimeline()
    {
        purge(0xffffffffffffffffUL);
    }
    void setID(uint64_t id){
        m_id  = id;
    }
    /*can be concurrent*/
    inline T * get(uint64_t originCheckPoint)
    {
        MetaInfo * current = m_current, *first;
        __asm__ __volatile__("lfence" ::: "memory");
        if (likely(current->begin <= originCheckPoint))
        {
            if (likely(current->end>originCheckPoint))
                return current;
            else
            {
                first = current;
                __asm__ __volatile__("lfence" ::: "memory");
                MetaInfo * newer = m_current;
                if (newer->end < originCheckPoint)
                    return NULL;
                while (newer != current)
                {
                    if (newer->begin < originCheckPoint)
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
                if (m->begin < originCheckPoint)
                    return m;
                else
                    m = m->prev;
            }
            return NULL;
        }
    }
    /*must be serial*/
    int put(T * meta, uint64_t originCheckPoint)
    {
        MetaInfo * m = new MetaInfo;
        m->begin  = originCheckPoint;
        m->end.fileID = 0xffffffffffffffffUL;
        m->end.offset = 0xffffffffffffffffUL;
        m->meta = meta;
        meta->m_id = m_id|(m_version++);
        if (m_current == NULL)
        {
            __asm__ __volatile__("sfence" ::: "memory");
            m_current = m;
            return 0;
        }
        else
        {
            if (m_current->begin<m->begin)
            {
                delete m;
                return -1;
            }
            m->prev = m_current;
            m_current->end = originCheckPoint;
            __asm__ __volatile__("sfence" ::: "memory");
            m_current = m;
            return 0;
        }
    }
    int disableCurrent(uint64_t originCheckPoint)
    {
        return put(NULL, originCheckPoint);
    }
    void purge(uint64_t originCheckPoint)
    {
        MetaInfo * m = m_current;
        while (m != NULL)
        {
            if (originCheckPoint < m->end)
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
    trieTree tables;
    uint64_t m_id;
    std::string charset;
};
metaDataCollection::metaDataCollection() :
        m_dbs(),m_maxTableId(0)
{
    m_SqlParser = new sqlParser::sqlParser();
    m_charsetSizeList.insert(std::pair<const char*,int>("big5", 2));
    m_charsetSizeList.insert(std::pair<const char*,int>("dec8", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("cp850", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("hp8",  1));
    m_charsetSizeList.insert(std::pair<const char*,int>("koi8r",  1));
    m_charsetSizeList.insert(std::pair<const char*,int>("latin1",  1));
    m_charsetSizeList.insert(std::pair<const char*,int>("latin2",  1));
    m_charsetSizeList.insert(std::pair<const char*,int>("swe7",  1));
    m_charsetSizeList.insert(std::pair<const char*,int>("ascii",  1));
    m_charsetSizeList.insert(std::pair<const char*,int>("ujis", 3));
    m_charsetSizeList.insert(std::pair<const char*,int>("sjis",  2));
    m_charsetSizeList.insert(std::pair<const char*,int>("hebrew", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("tis620", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("euckr",  2));
    m_charsetSizeList.insert(std::pair<const char*,int>("koi8u", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("gb2312", 2));
    m_charsetSizeList.insert(std::pair<const char*,int>("greek", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("cp1250", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("gbk", 2));
    m_charsetSizeList.insert(std::pair<const char*,int>("latin5", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("armscii8", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("utf8", 3));
    m_charsetSizeList.insert(std::pair<const char*,int>("ucs2", 2));
    m_charsetSizeList.insert(std::pair<const char*,int>("cp866", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("keybcs2", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("macce", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("macroman", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("cp852", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("latin7", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("utf8mb4", 4));
    m_charsetSizeList.insert(std::pair<const char*,int>("cp1251", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>("utf16", 4));
    m_charsetSizeList.insert(std::pair<const char*,int>("utf16le", 4));
    m_charsetSizeList.insert(std::pair<const char*,int>("cp1256", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>( "cp1257", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>( "utf32", 4));
    m_charsetSizeList.insert(std::pair<const char*,int>( "binary", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>( "geostd8", 1));
    m_charsetSizeList.insert(std::pair<const char*,int>( "cp932", 2));
    m_charsetSizeList.insert(std::pair<const char*,int>( "eucjpms", 3));
    m_charsetSizeList.insert(std::pair<const char*,int>( "gb18030", 4));
}
metaDataCollection::~metaDataCollection()
{
    if (m_SqlParser != NULL)
        delete m_SqlParser;
}
tableMeta * metaDataCollection::get(const char * database, const char * table,
        uint64_t originCheckPoint)
{
    MetaTimeline<dbInfo> * db =
            static_cast<MetaTimeline<dbInfo>*>(m_dbs.findNCase(
                    (const unsigned char*) database));
    if (db == NULL)
        return NULL;
    dbInfo * currentDB = db->get(originCheckPoint);
    if (currentDB == NULL)
        return NULL;
    MetaTimeline<tableMeta> * metas =
            static_cast<MetaTimeline<tableMeta>*>(currentDB->tables.findNCase(
                    (const unsigned char*) table));
    if (metas == NULL)
        return NULL;
    return metas->get(originCheckPoint);
}
tableMeta *metaDataCollection::get(uint64_t tableID){
    tableMetaWrap t = {tableID,nullptr};
    leveldb::SkipList<tableMetaWrap*, tableIDComparator>::Iterator iter(m_allTables);
    iter.Seek(&t);
    if(iter.Valid())
        return iter.key()->meta;
    else
        return nullptr;
}
int metaDataCollection::put(const char * database, const char * table,
        tableMeta * meta, uint64_t originCheckPoint)
{
    MetaTimeline<dbInfo> * db =
            static_cast<MetaTimeline<dbInfo>*>(m_dbs.findNCase(
                    (const unsigned char*) database));
    bool newMeta = false;
    if (db == NULL)
        return -1;
    dbInfo * currentDB = db->get(originCheckPoint);
    MetaTimeline<tableMeta> * metas =
            static_cast<MetaTimeline<tableMeta>*>(currentDB->tables.findNCase(
                    (const unsigned char*) table));
    if (metas == NULL)
    {
        newMeta = true;
        metas = new MetaTimeline<tableMeta>(meta, originCheckPoint);
        metas->setID(m_maxTableId++);
    }
    else
    {
        metas->put(meta, originCheckPoint);
    }
    if (newMeta)
    {
        __asm__ __volatile__("mfence" ::: "memory");
        currentDB->tables.insert((const unsigned char*) table, metas);
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
        uint64_t originCheckPoint)
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
            CharsetTree::iterator citer = m_charsetSizeList.find(column.m_charset.c_str());
            if (citer == m_charsetSizeList.end())
            {
                printf("unknown charset %s\n", column.m_charset.c_str());
                delete meta;
                return -1;
            }
            column.m_size *= citer->second;
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
            != put(newTable.database.c_str(), newTable.table.c_str(), meta,originCheckPoint))
    {
        printf("insert new meta of table %s.%s failed",
                newTable.database.c_str(), newTable.table.c_str());
        delete meta;
        return -1;
    }
    return 0;
}
int metaDataCollection::createTableLike(handle * h, const newTableInfo *t,
        uint64_t originCheckPoint)
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
            likedTable.table.c_str(), 0xffffffffffffffffUL);
    if (likedMeta == NULL)
    {
        printf("create liked table %s.%s is not exist",
                likedTable.database.c_str(), likedTable.table.c_str());
        return -1;
    }
    tableMeta * meta = new tableMeta;
    *meta = *likedMeta;
    meta->m_tableName = newTable.table;
    if (0
            != put(newTable.database.c_str(), newTable.table.c_str(), meta,
                    originCheckPoint))
    {
        printf("insert new meta of table %s.%s failed",
                newTable.database.c_str(), newTable.table.c_str());
        delete meta;
        return -1;
    }
    return 0;
}
int metaDataCollection::alterTable(handle * h, const newTableInfo *t,
        uint64_t originCheckPoint)
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
            0xffffffffffffffffUL);
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
            CharsetTree::iterator citer = m_charsetSizeList.find(column.m_charset.c_str());
            if(citer == m_charsetSizeList.end())
            {
                printf("unknown charset %s\n", column.m_charset.c_str());
                delete newMeta;
                return -1;
            }
            column.m_size *= citer->second;
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
                    originCheckPoint))
    {
        printf("insert new meta of table %s.%s failed",
                newTable.database.c_str(), newTable.table.c_str());
        delete meta;
        return -1;
    }
    return 0;
}

int metaDataCollection::processNewTable(handle * h, const newTableInfo *t,
        uint64_t originCheckPoint)
{
    if (t->type == newTableInfo::CREATE_TABLE)
    {
        if (t->createLike)
        {
            return createTableLike(h, t, originCheckPoint);
        }
        else
        {
            return createTable(h, t, originCheckPoint);
        }
    }
    else if (t->type == newTableInfo::ALTER_TABLE)
    {
        return alterTable(h, t, originCheckPoint);
    }
    else
        return -1;
}
int metaDataCollection::processOldTable(handle * h, const Table *table,
        uint64_t originCheckPoint)
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
    dbInfo * currentDB = db->get(originCheckPoint);
    if (currentDB == NULL)
        return -1;
    MetaTimeline<tableMeta> * metas =
            static_cast<MetaTimeline<tableMeta>*>(currentDB->tables.findNCase(
                    (const unsigned char*) table));
    if (metas == NULL)
        return -1;
    return metas->disableCurrent(originCheckPoint);
}
int metaDataCollection::processDatabase(const databaseInfo * database,
        uint64_t originCheckPoint)
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
            db = new MetaTimeline<dbInfo>(current, originCheckPoint);
            __asm__ __volatile__("mfence" ::: "memory");
            if (0
                    != m_dbs.insert(
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

    if (NULL == (current = db->get(originCheckPoint)))
    {
        if (database->type == databaseInfo::CREATE_DATABASE)
        {
            current = new dbInfo;
            current->charset = database->charset;
            if (current->charset.empty())
                current->charset = m_defaultCharset;
            __asm__ __volatile__("mfence" ::: "memory");
            if (0 != db->put(current, originCheckPoint))
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
        return db->disableCurrent(originCheckPoint);
    }
    else
        return -1;
}

int metaDataCollection::processDDL(const char * ddl, uint64_t originCheckPoint)
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
        metaChangeInfo * meta = static_cast<metaChangeInfo*>(currentHandle->userData);
        if (meta->database.type
                != databaseInfo::MAX_DATABASEDDL_TYPE)
        {
            processDatabase(&meta->database, originCheckPoint);
        }
        for (list<newTableInfo*>::const_iterator iter =
                meta->newTables.begin();
                iter != meta->newTables.end(); iter++)
        {
            processNewTable(currentHandle, *iter, originCheckPoint);
        }
        for (list<Table>::const_iterator iter = meta->oldTables;
                iter != meta->oldTables.end(); iter++)
        {
            processOldTable(currentHandle, &(*iter), originCheckPoint);
        }
    }
    return 0;
}
int metaDataCollection::purge(uint64_t originCheckPoint)
{
    for(trieTree::iterator iter = m_dbs.begin();iter.valid();iter.next())
    {
        MetaTimeline<dbInfo> * db = static_cast<MetaTimeline<dbInfo>*>(iter.value() );
        if(db == NULL)
            continue;
        db->purge(originCheckPoint);
        dbInfo * dbMeta = db->get(0xffffffffffffffffUL);
        if(dbMeta == NULL)
            continue;
        for(trieTree::iterator titer = dbMeta->tables.begin();titer.valid();titer.next())
        {
            MetaTimeline<tableMeta> * table = static_cast<MetaTimeline<tableMeta>*>(titer.value() );
            if(table == NULL)
                continue;
            table->purge(originCheckPoint);
        }
    }
    return 0;
}

