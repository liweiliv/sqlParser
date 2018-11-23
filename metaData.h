/*
 * metaData.h
 *
 *  Created on: 2018年11月11日
 *      Author: liwei
 */

#ifndef _METADATA_H_
#define _METADATA_H_
#include <string>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <list>
#include <assert.h>
struct stringArray
{
    char ** m_array;
    uint32_t m_Count;
    stringArray():m_array(NULL),m_Count(0){}
    ~stringArray()
    {
        clean();
    }
    void clean()
    {
        if(m_Count>0)
        {
            for(int i=0;i<m_Count;i++)
                free(m_array[i]);
            free(m_array);
        }
        m_Count = 0;
        m_array = NULL;

    }
    stringArray &operator =(const stringArray &c)
    {
        m_Count = c.m_Count;
        if(m_Count>0)
        {
            for(int i=0;i<m_Count;i++)
            {
                uint32_t size = strlen(c.m_array[i]);
                m_array[i] = (char*)malloc(size+1);
                memcpy(m_array[i],c.m_array[i],size);
                m_array[i][size] = '\0';
            }
        }
        else
            m_array = NULL;
        return *this;
    }
    stringArray &operator =(const std::list<std::string> &l)
    {
        clean();
        m_array = (char**)malloc(sizeof(char*)*l.size());
        for(std::list<std::string>::const_iterator iter = l.begin();iter!=l.end();iter++)
        {
            m_array[m_Count] = (char*)malloc((*iter).size()+1);
            memcpy(m_array[m_Count],(*iter).c_str(),(*iter).size());
            m_array[m_Count][(*iter).size()] = '\0';
        }
        return *this;
    }

};
struct columnMeta
{
    uint8_t m_columnType;
    uint16_t m_columnIndex;
    std::string m_columnName;
    std::string m_charset;
    uint32_t m_size;
    uint32_t m_precision;
    uint32_t m_decimals;
    stringArray m_setAndEnumValueList;
    bool m_signed;
    bool m_isPrimary;
    bool m_isUnique;
    bool m_generated;
    columnMeta():m_columnType(0),m_columnIndex(0),m_size(0),m_precision(0),m_decimals(0),
            m_setAndEnumValueList(NULL),m_signed(false),m_isPrimary(false),m_isUnique(false),m_generated(false)
    {}
    columnMeta &operator =(const columnMeta &c)
    {
        m_columnType = c.m_columnType;
        m_columnIndex = c.m_columnIndex;
        m_columnName = c.m_columnName;
        m_charset = c.m_charset;
        m_size = c.m_size;
        m_precision = c.m_precision;
        m_decimals =c.m_decimals;
        m_setAndEnumValueList = c.m_setAndEnumValueList;
        m_signed = c.m_signed;
        m_isPrimary = c.m_isPrimary;
        m_isUnique = c.m_isUnique;
        m_generated = c.m_generated;
        return *this;
    }
};
struct indexMeta
{
    stringArray columns;
    std::string name;
};
struct tableMeta
{
    std::string  m_tableName;
    std::string  m_charset;
    columnMeta * m_columns;
    uint32_t m_columnsCount;

    stringArray m_primaryKey;
    indexMeta * m_uniqueKeys;
    uint16_t m_uniqueKeysCount;
    tableMeta():m_columns(NULL),m_columnsCount(0),m_uniqueKeys(NULL),m_uniqueKeysCount(0)
    {
    }
    ~tableMeta()
    {
        if(m_columns)
            delete []m_columns;
        if(m_uniqueKeysCount)
            delete []m_uniqueKeys;
    }
    tableMeta &operator =(const tableMeta &t)
    {
        m_tableName = t.m_tableName;
        m_charset = t.m_charset;
        if((m_columnsCount=t.m_columnsCount)>0)
        {
            m_columns = new columnMeta[m_columnsCount];
            for(int i =0 ;i<m_columnsCount;i++)
                m_columns[i] = t.m_columns[i];
        }
        else
            m_columns = NULL;
        m_primaryKey = t.m_primaryKey;
        if((m_uniqueKeysCount=t.m_uniqueKeysCount)>0)
        {
            m_uniqueKeys = new indexMeta[m_uniqueKeysCount];
            for(int i =0 ;i<m_uniqueKeysCount;i++)
            {
                m_uniqueKeys[i].columns = t.m_uniqueKeys[i].columns;
                m_uniqueKeys[i].name = t.m_uniqueKeys[i].name;
            }
        }
        else
            m_uniqueKeys = NULL;
        return *this;
    }
    columnMeta * getColumn(const char * columnName)
    {
        for(int i=0;i<m_columnsCount;i++)
        {
            if(strcmp(m_columns[i].m_columnName.c_str(),columnName)==0)
                return &m_columns[i];
        }
        return NULL;
    }
    indexMeta *getUniqueKey(const char *UniqueKeyname)
    {
        for(int i=0;i<m_uniqueKeysCount;i++)
        {
            if(strcmp(m_uniqueKeys[i].name.c_str(),UniqueKeyname)==0)
                return &m_columns[i];
        }
        return NULL;
    }
    int dropColumn(uint32_t columnIndex)
    {
        if(columnIndex>=m_columnsCount)
            return -1;
        columnMeta * columns = new columnMeta[m_columnsCount-1];
        for(uint32_t idx = 0;idx<columnIndex;idx++)
            columns[idx]=m_columns[idx];
        for(uint32_t idx = m_columnsCount-1;idx>columnIndex;idx--)
        {
            columns[idx-1]=m_columns[idx];
            columns[idx-1].m_columnIndex--;
        }
        delete []m_columns;
        m_columns = columns;
        m_columnsCount--;
        return 0;
    }
    int dropColumn(const char *column)
    {
        columnMeta * d = getColumn(column);
        if(d == NULL)
            return -1;
        return dropColumn(d->m_columnIndex);
    }
    int addColumn(const columnMeta* column,const char * addAfter = NULL)
    {
        if(getColumn(column->m_columnName.c_str())!=NULL)
            return -1;
        if(addAfter)
        {
            columnMeta * before = getColumn(addAfter);
            if(before == NULL)
                return -2;
            columnMeta * columns = new columnMeta[m_columnsCount+1];
            column->m_columnIndex = before->m_columnIndex+1;
            for(uint32_t idx = 0;idx<=before->m_columnIndex;idx++)
                columns[idx]=m_columns[idx];
            columns[column->m_columnIndex] = *column;
            for(uint32_t idx = column->m_columnIndex+1;idx<=m_columnsCount;idx++)
            {
                columns[idx]=m_columns[idx-1];
                columns->m_columnIndex++;
            }
        }
        else
        {
            columnMeta * columns = new columnMeta[m_columnsCount+1];
            for(uint32_t idx = 0;idx<=m_columnsCount;idx++)
                columns[idx]=m_columns[idx-1];
            column->m_columnIndex = m_columnsCount;
            columns[m_columnsCount] = column;
        }
        m_columnsCount++;
        return 0;
    }
    int dropPrimaryKey()
    {
        if(m_primaryKey.m_Count==0)
            return -1;
        for(uint32_t idx = 0;idx<m_primaryKey.m_Count;idx++)
        {
            columnMeta * c = getColumn(m_primaryKey.m_array[idx]);
            assert(c != NULL);
            c->m_isPrimary = false;
        }
        m_primaryKey.clean();
        return 0;
    }
    int createPrimaryKey(const std::list<std::string> &columns)
    {
        if(m_primaryKey.m_Count!=0)
            return -1;
        m_primaryKey = columns;
        for(uint32_t idx = 0;idx<m_primaryKey.m_Count;idx++)
        {
            columnMeta * c = getColumn(m_primaryKey.m_array[idx]);
            if(c == NULL)
            {
                m_primaryKey.clean();
                return -1;
            }
            c->m_isPrimary = true;
        }
        return 0;
    }
    int dropUniqueKey(const char *ukName)
    {
        int idx=0;
        for(;idx<m_uniqueKeysCount;idx++)
        {
            if(strcmp(m_uniqueKeys[idx].name.c_str(),ukName)==0)
                goto DROP;
        }
        return -1;
DROP:
        indexMeta * newIndex = new indexMeta[m_uniqueKeysCount-1];
        for(int i=0;i<idx;i++)
        {
            newIndex[i].name = m_uniqueKeys[i].name;
            newIndex[i].columns = m_uniqueKeys[i].columns;
        }
        for(int i=idx+1;i<m_uniqueKeysCount;i++)
        {
            newIndex[i-1].name = m_uniqueKeys[i].name;
            newIndex[i-1].columns = m_uniqueKeys[i].columns;
        }
        for(int i = 0;i<m_uniqueKeys[idx].columns.m_Count;i++)
        {
            for(int j =0;j<m_uniqueKeysCount-1;j++)
            {
                for(int k = 0;k<newIndex[j].columns.m_Count;k++)
                {

                }
            }
        }
        m_uniqueKeys = newIndex;
        m_uniqueKeysCount--;
        return 0;
    }
    int addUniqueKey(const char *ukName,const std::list<std::string> &columns)
    {

    }


};
#endif /* _METADATA_H_ */
