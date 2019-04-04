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
#include "mysqlTypes.h"
#include "message/record.h"
#include "charset.h"
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
            for(uint32_t i=0;i<m_Count;i++)
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
            for(uint32_t i=0;i<m_Count;i++)
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
    uint8_t m_columnType; //type in DBStream
	uint8_t m_srcColumnType;// type in database
    uint16_t m_columnIndex;  //column id in table
    std::string m_columnName;
    const charsetInfo* m_charset;
    uint32_t m_size;
    uint32_t m_precision;
    uint32_t m_decimals;
    stringArray m_setAndEnumValueList;
    bool m_signed;
    bool m_isPrimary;
    bool m_isUnique;
    bool m_generated;
    columnMeta():m_columnType(0), m_srcColumnType(0),m_columnIndex(0),m_size(0),m_precision(0),m_decimals(0),
            m_setAndEnumValueList(),m_signed(false),m_isPrimary(false),m_isUnique(false),m_generated(false)
    {}
    columnMeta &operator =(const columnMeta &c)
    {
        m_columnType = c.m_columnType;
		m_srcColumnType = c.m_srcColumnType;
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
    std::string toString()
    {
        std::string sql("`");
        sql.append(m_columnName).append("` ");
        char numBuf[40] = {0};
        switch(m_srcColumnType)
        {
        case MYSQL_TYPE_DECIMAL:
            sql.append("DECIMAL").append("(");
            sprintf(numBuf,"%u",m_size);
            sql.append(numBuf).append(",");
            sprintf(numBuf,"%u",m_decimals);
            sql.append(numBuf).append(")");
            break;
        case MYSQL_TYPE_DOUBLE:
            sql.append("DOUBLE").append("(");
            sprintf(numBuf,"%u",m_size);
            sql.append(numBuf).append(",");
            sprintf(numBuf,"%u",m_decimals);
            sql.append(numBuf).append(")");
            break;
        case MYSQL_TYPE_FLOAT:
            sql.append("FLOAT").append("(");
            sprintf(numBuf,"%u",m_size);
            sql.append(numBuf).append(",");
            sprintf(numBuf,"%u",m_decimals);
            sql.append(numBuf).append(")");
            break;
        case MYSQL_TYPE_BIT:
            sql.append("BIT").append("(");
            sprintf(numBuf,"%u",m_size);
            sql.append(numBuf).append(")");
            break;
        case MYSQL_TYPE_TINY:
            sql.append("TINY");
            if(!m_signed)
                sql.append(" UNSIGNED");
            break;
        case MYSQL_TYPE_SHORT:
            sql.append("SMALLINT");
            if(!m_signed)
                sql.append(" UNSIGNED");
            break;
        case MYSQL_TYPE_INT24:
            sql.append("MEDIUMINT");
            if(!m_signed)
                sql.append(" UNSIGNED");
            break;
        case MYSQL_TYPE_LONG:
            sql.append("INTEGER");
            if(!m_signed)
                sql.append(" UNSIGNED");
            break;
        case MYSQL_TYPE_LONGLONG:
            sql.append("BIGINT");
            if(!m_signed)
                sql.append(" UNSIGNED");
            break;
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
            sql.append("DATETIME");
            if(m_precision>0)
            {
                sprintf(numBuf,"%u",m_precision);
                sql.append("(").append(numBuf).append(")");
            }
            break;
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            sql.append("TIMESTAMP");
            if(m_precision>0)
            {
                sprintf(numBuf,"%u",m_precision);
                sql.append("(").append(numBuf).append(")");
            }
            break;
        case MYSQL_TYPE_DATE:
            sql.append("DATE");
            break;
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
            sql.append("TIME");
            if(m_precision>0)
            {
                sprintf(numBuf,"%u",m_precision);
                sql.append("(").append(numBuf).append(")");
            }
            break;
        case MYSQL_TYPE_YEAR:
            sql.append("YEAR");
            if(m_precision>0)
            {
                sprintf(numBuf,"%u",m_precision);
                sql.append("(").append(numBuf).append(")");
            }
            break;
        case MYSQL_TYPE_STRING:
            sprintf(numBuf,"%u",m_size);
            if(m_charset!=nullptr)
            {
                sql.append("BINARY").append("(").append(numBuf).append(")");
            }
            else
            {
                sql.append("CHAR").append("(").append(numBuf).append(") CHARACTER SET ").append(m_charset->name);
            }
            break;
        case MYSQL_TYPE_VARCHAR:
            sql.append("VARCHAR").append("(").append(numBuf).append(") CHARACTER SET ").append(m_charset->name);
            break;
        case MYSQL_TYPE_VAR_STRING:
            sprintf(numBuf,"%u",m_size);
            if(m_charset!=nullptr)
            {
                sql.append("VARBINARY").append("(").append(numBuf).append(")");
            }
            else
            {
                sql.append("VARCHAR").append("(").append(numBuf).append(") CHARACTER SET").append(m_charset->name);
            }
            break;
        case MYSQL_TYPE_TINY_BLOB:
            if(m_charset!=nullptr)
                sql.append("TINYTEXT").append(" CHARACTER SET ").append(m_charset->name);
            else
                sql.append("TINYBLOB");
            break;
        case MYSQL_TYPE_MEDIUM_BLOB:
            if(m_charset!=nullptr)
                sql.append("MEDIUMTEXT").append(" CHARACTER SET ").append(m_charset->name);
            else
                sql.append("MEDIUMBLOB");
            break;
        case MYSQL_TYPE_BLOB:
            if(m_charset!=nullptr)
                sql.append("TEXT").append(" CHARACTER SET ").append(m_charset->name);
            else
                sql.append("BLOB");
            break;
        case MYSQL_TYPE_LONG_BLOB:
            if(m_charset!=nullptr)
                sql.append("LONGTEXT").append(" CHARACTER SET ").append(m_charset->name);
            else
                sql.append("LONGBLOB");
            break;
        case MYSQL_TYPE_ENUM:
        {
            sql.append("ENUM (");
            for(uint32_t idx =0 ;idx<m_setAndEnumValueList.m_Count;idx++)
            {
                if(idx>0)
                    sql.append(",");
                sql.append("'").append(m_setAndEnumValueList.m_array[idx]).append("'");
            }
            sql.append(")").append(" CHARACTER SET ").append(m_charset->name);
            break;
        }
        case MYSQL_TYPE_SET:
        {
            sql.append("SET (");
            for(uint32_t idx =0 ;idx<m_setAndEnumValueList.m_Count;idx++)
            {
                if(idx>0)
                    sql.append(",");
                sql.append("'").append(m_setAndEnumValueList.m_array[idx]).append("'");
            }
            sql.append(")").append(" CHARACTER SET ").append(m_charset->name);
            break;
        }
        case MYSQL_TYPE_GEOMETRY:
            sql.append("GEOMETRY");
            break;
        case MYSQL_TYPE_JSON:
            sql.append("JSON");
            break;
        default:
            abort();
        }
        return sql;
    }
};
struct keyInfo
{
    std::string name;
	uint16_t count;
	uint16_t *keyIndexs;
	keyInfo() :count(0), keyIndexs(nullptr) {}
	keyInfo(const keyInfo & key) :name(key.name), count(key.count), keyIndexs(nullptr){
		if (count > 0)
		{
			keyIndexs = new uint16_t[count];
			memcpy(keyIndexs, key.keyIndexs, sizeof(uint16_t)*count);
		}
	}
	void init(const char* name, uint16_t count, const uint16_t *keyIndexs)
	{
		this->count = count;
		this->name = name;
		if (count > 0)
		{
			this->keyIndexs = new uint16_t[count];
			memcpy(this->keyIndexs, keyIndexs, sizeof(uint16_t)*count);
		}
		else
			this->keyIndexs = nullptr;
	}
	keyInfo& operator =(const keyInfo &key)
	{
		name = key.name;
		count = key.count;
		if (count > 0)
		{
			keyIndexs = new uint16_t[count];
			memcpy(keyIndexs, key.keyIndexs, sizeof(uint16_t)*count);
		}
		else
			keyIndexs = nullptr;
		return *this;
	}
	void clean()
	{
		if (keyIndexs)
		{
			delete[]keyIndexs;
			keyIndexs = nullptr;
		}
		count = 0;
	}
	~keyInfo()
	{
		clean();
	}
};
struct tableMeta
{
	std::string  m_dbName;
    std::string  m_tableName;
    const charsetInfo *m_charset;
    columnMeta * m_columns;
    uint32_t m_columnsCount;
    uint64_t m_id;
	keyInfo m_primaryKey;
    uint16_t m_uniqueKeysCount;
	keyInfo * m_uniqueKeys;
	uint16_t m_indexCount;
	keyInfo * m_indexs;
    static inline uint16_t tableVersion(uint64_t tableIDInfo)
    {
        return tableIDInfo&0xffff;
    }
    static inline uint64_t tableID (uint64_t tableIDInfo)
    {
        return tableIDInfo&0xffffffffffff0000ul;
    }
    tableMeta():m_columns(NULL),m_columnsCount(0),m_id(0),m_uniqueKeysCount(0), m_uniqueKeys(nullptr), m_indexCount(0), m_indexs(nullptr)
    {
    }
	tableMeta(DATABASE_INCREASE::TableMetaMessage * msg):m_dbName(msg->database? msg->database:""),m_tableName(msg->table?msg->table:""), m_charset(&charsets[msg->metaHead.charsetId]), m_columnsCount(msg->metaHead.columnCount),
		m_id(msg->metaHead.tableMetaID), m_uniqueKeysCount(msg->metaHead.uniqueKeyCount)
	{
		m_columns = new columnMeta[m_columnsCount];
		for (uint32_t i = 0; i < m_columnsCount; i++)
		{
			m_columns[i].m_columnIndex = i;
			m_columns[i].m_columnName = msg->columnName(i);
			if (msg->columns[i].charsetID < charsetCount)
				m_columns[i].m_charset = &charsets[msg->columns[i].charsetID];
			else
				m_columns[i].m_charset = nullptr;
			m_columns[i].m_columnType = msg->columns[i].type;
			m_columns[i].m_srcColumnType = msg->columns[i].srcType;
			m_columns[i].m_decimals = msg->columns[i].decimals;
			m_columns[i].m_precision = msg->columns[i].precision;
			m_columns[i].m_generated = msg->columns[i].flag&COLUMN_FLAG_VIRTUAL;
			m_columns[i].m_signed = msg->columns[i].flag&COLUMN_FLAG_SIGNED;
			m_columns[i].m_size = msg->columns[i].size;
			if (msg->columns[i].setOrEnumInfoOffset != 0)
			{
				const char * base;
				uint16_t *valueList, valueListSize;
				msg->setOrEnumValues(i, base, valueList, valueListSize);
				m_columns[i].m_setAndEnumValueList.m_Count = valueListSize;
				m_columns[i].m_setAndEnumValueList.m_array = (char**)malloc(sizeof(char*)*valueListSize);
				for (uint16_t j = 0; j < valueListSize; j++)
				{
					m_columns[i].m_setAndEnumValueList.m_array[j] = (char*)malloc(valueList[j + 1] - valueList[j]);
					memcpy(m_columns[i].m_setAndEnumValueList.m_array[j], base + valueList[j], valueList[j + 1] - valueList[j]);
				}
			}
			m_columns[i].m_isPrimary = false;
			m_columns[i].m_isUnique = false;
		}
		if (msg->metaHead.primaryKeyColumnCount > 0)
		{
			m_primaryKey.init("primary key", msg->metaHead.primaryKeyColumnCount, msg->primaryKeys);
			for (uint16_t i = 0; i < m_primaryKey.count; i++)
				getColumn(m_primaryKey.keyIndexs[i])->m_isPrimary = true;
		}
		if (msg->metaHead.uniqueKeyCount > 0)
		{
			m_uniqueKeys = new keyInfo[msg->metaHead.uniqueKeyCount];
			for (uint16_t i = 0; i < msg->metaHead.uniqueKeyCount; i++)
			{
				m_uniqueKeys[i].init(msg->data + msg->uniqueKeyNameOffset[i], msg->uniqueKeyColumnCounts[i], msg->uniqueKeys[i]);
				for (uint16_t j = 0; j < m_uniqueKeys[i].count; j++)
					getColumn(m_uniqueKeys[i].keyIndexs[j])->m_isUnique = true;
			}
			m_uniqueKeysCount = msg->metaHead.uniqueKeyCount;
		}
	}
	const char * createTableMetaRecord()
	{

	}
    ~tableMeta()
    {
        if(m_columns)
            delete []m_columns;
		if (m_uniqueKeys !=nullptr)
			delete[]m_uniqueKeys;
		if (m_indexs != nullptr)
			delete[]m_indexs;
    }
    tableMeta &operator =(const tableMeta &t)
    {
        m_tableName = t.m_tableName;
        m_charset = t.m_charset;
        if((m_columnsCount=t.m_columnsCount)>0)
        {
            m_columns = new columnMeta[m_columnsCount];
            for(uint32_t i =0 ;i<m_columnsCount;i++)
                m_columns[i] = t.m_columns[i];
        }
        else
            m_columns = NULL;
        m_id = t.m_id;
        /*copy primary key*/
		m_primaryKey = t.m_primaryKey;
        /*copy unique key*/
		m_uniqueKeysCount = t.m_uniqueKeysCount;
        if(t.m_uniqueKeys!=nullptr)
        {
			m_uniqueKeys = new keyInfo[m_uniqueKeysCount];
            for(int i =0 ;i<m_uniqueKeysCount;i++)
				m_uniqueKeys[i] = t.m_uniqueKeys[i];
        }
		m_indexCount = t.m_indexCount;
		if (t.m_indexs != nullptr)
		{
			m_indexs = new keyInfo[m_indexCount];
			for (int i = 0; i < m_indexCount; i++)
				m_indexs[i] = t.m_indexs[i];
		}
        return *this;
    }
	inline columnMeta *getColumn(uint16_t idx)
	{
		if (idx > m_columnsCount)
			return nullptr;
		return &m_columns[idx];
	}
    columnMeta * getColumn(const char * columnName)
    {
        for(uint32_t i=0;i<m_columnsCount;i++)
        {
            if(strcmp(m_columns[i].m_columnName.c_str(),columnName)==0)
                return &m_columns[i];
        }
        return NULL;
    }
    keyInfo *getUniqueKey(const char *UniqueKeyname)
    {
        for(uint16_t i=0;i<m_uniqueKeysCount;i++)
        {
            if(strcmp(m_uniqueKeys[i].name.c_str(),UniqueKeyname)==0)
                return &m_uniqueKeys[i];
        }
        return NULL;
    }
    int dropColumn(uint32_t columnIndex)//todo ,update key
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
        if(m_uniqueKeys !=nullptr)
        {
            for(uint16_t idx = 0;idx<m_uniqueKeysCount;idx++)
            {
                for(uint16_t i = 0;i<m_uniqueKeys[idx].count;)
                {
                    if(m_uniqueKeys[idx].keyIndexs[i]>columnIndex)
						m_uniqueKeys[idx].keyIndexs[i]--;
					else if (m_uniqueKeys[idx].keyIndexs[i] == columnIndex)
					{
						memcpy(&m_uniqueKeys[idx].keyIndexs[i], &m_uniqueKeys[idx].keyIndexs[i + 1], sizeof(uint16_t)*(m_uniqueKeys[idx].count - i - 1));
						m_uniqueKeys[idx].count--;
						continue;//do not do [i++]
					}
					i++;
                }
				if (m_uniqueKeys[idx].count == 0)
				{
					dropUniqueKey(m_uniqueKeys[idx].name.c_str());
				}
            }
        }
		for (uint16_t i = 0; i < m_primaryKey.count;)
		{
			if (m_primaryKey.keyIndexs[i] > columnIndex)
				m_primaryKey.keyIndexs[i]--;
			else if (m_primaryKey.keyIndexs[i] == columnIndex)
			{
				memcpy(&m_primaryKey.keyIndexs[i], &m_primaryKey.keyIndexs[i + 1], sizeof(uint16_t)*(m_primaryKey.count - i - 1));
				m_primaryKey.count--;
				continue;//do not do [i++]
			}
			i++;
		}
		if (m_primaryKey.count == 0)
			dropPrimaryKey();
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
            for(uint32_t idx = 0;idx<=before->m_columnIndex;idx++)
                columns[idx]=m_columns[idx];
            columns[column->m_columnIndex] = *column;
            columns[column->m_columnIndex].m_columnIndex = before->m_columnIndex+1;
            for(uint32_t idx = column->m_columnIndex+1;idx<=m_columnsCount;idx++)
            {
                columns[idx]=m_columns[idx-1];
                columns->m_columnIndex++;
            }
            if(m_uniqueKeys!=nullptr)
            {
                for(uint16_t idx = 0;idx<m_uniqueKeysCount;idx++)
                {
                    for(uint16_t i = 0;i<m_uniqueKeys[idx].count;i++)
                    {
                        if(m_uniqueKeys[idx].keyIndexs[i]>before->m_columnIndex)
							m_uniqueKeys[idx].keyIndexs[i]++;
                    }
                }
            }
            if(m_primaryKey.count>0)
            {
                for(uint16_t i = 0;i< m_primaryKey.count;i++)
                {
                    if(m_primaryKey.keyIndexs[i]>before->m_columnIndex)
						m_primaryKey.keyIndexs[i]++;
                }
            }
        }
        else
        {
            columnMeta * columns = new columnMeta[m_columnsCount+1];
            for(uint32_t idx = 0;idx<=m_columnsCount;idx++)
                columns[idx]=m_columns[idx-1];
            columns[m_columnsCount] = *column;
            columns[m_columnsCount].m_columnIndex = m_columnsCount;
        }
        m_columnsCount++;
        return 0;
    }
    int dropPrimaryKey()
    {
		for(uint16_t i=0;i< m_primaryKey.count;i++)
			getColumn(m_primaryKey.keyIndexs[i])->m_isPrimary = false;
		m_primaryKey.clean();
		return 0;
    }
    int createPrimaryKey(const std::list<std::string> &columns)
    {
        if(m_primaryKey.count !=0)
            return -1;
		m_primaryKey.clean();
		m_primaryKey.name = "primary key";
		m_primaryKey.keyIndexs = new uint16_t[ columns.size()];
        for(std::list<std::string>::const_iterator iter= columns.begin();iter!= columns.end();iter++)
        {
            columnMeta * c = getColumn((*iter).c_str());
            if(c == NULL)
            {
				m_primaryKey.clean();
                return -1;
            }
            c->m_isPrimary = true;
			m_primaryKey.keyIndexs[m_primaryKey.count++] = c->m_columnIndex;
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
		keyInfo * newUks = new keyInfo[m_uniqueKeysCount - 1];
		for (int i = 0; i < idx; i++)
			newUks[i] = m_uniqueKeys[i];
		for (int i = idx+1; i < m_uniqueKeysCount - 1; i++)
			newUks[i-1] = m_uniqueKeys[i];

        /*update columns */
        for(uint16_t i = 0;i< m_uniqueKeys[idx].count;i++)
        {
            for(uint16_t j =0;j<m_uniqueKeysCount-1;j++)
            {
                for(uint32_t k = 0;k<newUks[j].count;k++)
                {
					if(m_uniqueKeys[idx].keyIndexs[i] == newUks[j].keyIndexs[k])
                        goto COLUMN_IS_STILL_UK;
                }
            }
            getColumn(m_uniqueKeys[idx].keyIndexs[i])->m_isUnique = false;
COLUMN_IS_STILL_UK:
			continue;
        }
		delete[]m_uniqueKeys;
		m_uniqueKeys = newUks;
		m_uniqueKeysCount--;
        return 0;
    }
    int addUniqueKey(const char *ukName,const std::list<std::string> &columns)
    {
        if(getUniqueKey(ukName)!=NULL)
            return -1;
        /*copy data*/
		keyInfo * newUks = new keyInfo[m_uniqueKeysCount - 1];
		newUks[m_uniqueKeysCount].keyIndexs = new uint16_t[columns.size()];
		newUks[m_uniqueKeysCount].name = ukName;
		for (std::list<std::string>::const_iterator iter = columns.begin(); iter != columns.end(); iter++)
		{
			columnMeta * column = getColumn((*iter).c_str());
			if (column == nullptr)
			{
				delete[]newUks;
				return -1;
			}
			newUks[m_uniqueKeysCount].keyIndexs[newUks[m_uniqueKeysCount].count++] = column->m_columnIndex;
			if (!column->m_isUnique)
				column->m_isUnique = true;
		}
		for (int i = 0; i < m_uniqueKeysCount; i++)
			newUks[i] = m_uniqueKeys[i];
		delete []m_uniqueKeys;
		m_uniqueKeys = newUks;
		m_uniqueKeysCount++;
		return 0;
    }
    std::string toString()
    {
        std::string sql("CREATE TABLE `");
        sql.append(m_tableName).append("` (");
        for(uint32_t idx =0;idx<m_columnsCount;idx++)
        {
            if(idx!=0)
                sql.append(",\n");
            sql.append(m_columns[idx].toString());
        }
        if(m_primaryKey.count >0)
        {
            sql.append(",\n");
            sql.append("PRIMARY KEY (");
            for(uint32_t idx =0 ;idx< m_primaryKey.count;idx++)
            {
                if(idx>0)
                    sql.append(",");
                sql.append("`").append(getColumn(m_primaryKey.keyIndexs[idx])->m_columnName).append("`");
            }
            sql.append(")");
        }
        if(m_uniqueKeysCount>0)
        {
            for(int idx = 0;idx<m_uniqueKeysCount;idx++)
            {
                sql.append(",\n");
                sql.append("UNIQUE KEY `").append(m_uniqueKeys[idx].name).append("` (");
                for(uint32_t j =0 ;j<m_uniqueKeys[idx].count;j++)
                {
                    if(j>0)
                        sql.append(",");
                    sql.append("`").append(getColumn(m_uniqueKeys[idx].keyIndexs[j])->m_columnName).append("`");
                }
                sql.append(")");
            }
        }
        sql.append(".\n) ").append("CHARACTER SET").append(m_charset->name).append(";");
        return sql;
    }
};
#endif /* _METADATA_H_ */
