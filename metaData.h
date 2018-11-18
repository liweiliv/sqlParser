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
struct columnMeta
{
    uint8_t m_columnType;
    uint16_t m_columnIndex;
    std::string m_columnName;
    std::string m_charset;
    uint32_t m_size;
    uint32_t m_precision;
    uint32_t m_decimals;
    uint16_t m_setAndEnumValueListSize;
    char ** m_setAndEnumValueList;
    bool m_signed;
    bool m_isPrimary;
    bool m_isUnique;
    bool m_generated;
    columnMeta():m_columnType(0),m_columnIndex(0),m_size(0),m_precision(0),m_decimals(0),m_setAndEnumValueListSize(0),
            m_setAndEnumValueList(NULL),m_signed(false),m_isPrimary(false),m_isUnique(false),m_generated(false)
    {}
    ~columnMeta(){for(int idx = 0;idx<m_setAndEnumValueListSize;idx++){if(m_setAndEnumValueList[idx]!=NULL)delete m_setAndEnumValueList[idx];}}
};
struct columns
{
    columnMeta ** m_columns;
    uint16_t m_columnsCount;
};
struct tableMeta
{
    std::string  m_tableName;
    std::string  m_charset;
    columns m_columns;
    columns m_primaryKey;
    columns * m_uniqueKeys;
    uint16_t m_uniqueKeysCount;
    tableMeta()
    {
        memset(&m_primaryKey,0,sizeof(m_primaryKey));
        memset(&m_columns,0,sizeof(m_columns));
        m_uniqueKeys = NULL;
        m_uniqueKeysCount = 0;
    }
};
#endif /* _METADATA_H_ */
