/*
 * sqlParserFuncs.cpp
 *
 *  Created on: 2018年11月19日
 *      Author: liwei
 */
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <list>
#include <stdint.h>
using namespace std;
class parseStatus
{
public:
	enum parseStatustype
	{
		parseDatabase,
		parseTable,
		parseColumn,
		parseKeys,
	};
	parseStatustype type;
	parseStatus(parseStatustype t):type(t)
	{
	}
};
class parseStatusTable;
class parseStatusColumn:public parseStatus
{
    parseStatusTable * parent;
    uint8_t m_columnType;
    uint16_t m_columnIndex;
    std::string m_columnAfter;
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
    parseStatusColumn():parseStatus(parseColumn),parent(NULL),m_columnType(0),m_columnIndex(0),m_size(0),m_precision(0),m_decimals(0),m_setAndEnumValueListSize(0),
            m_setAndEnumValueList(NULL),m_signed(false),m_isPrimary(false),m_isUnique(false),m_generated(false)
    {}
    ~parseStatusColumn(){for(int idx = 0;idx<m_setAndEnumValueListSize;idx++){if(m_setAndEnumValueList[idx]!=NULL)delete m_setAndEnumValueList[idx];}}

};
class parseStatusKey:public parseStatus
{
	enum Keytypes{
		PRIMARY_KEY,
		UNIQUE_KEY,
		FOREIGN_KEY,
		NORMAL_KEY
	};
	Keytypes kType;
	std::string name;
	std::list<std::string> columnes;
	parseStatusTable * parent;
};
class parseStatusDatabase;
class parseStatusTable:public parseStatus
{

};
class parseStatusDatabase:public parseStatus
{
	list<string> newDatabases;
	string oldDatabase;
};



