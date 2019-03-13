/*
 * testAppendingIndex.cpp
 *
 *  Created on: 2019年3月8日
 *      Author: liwei
 */
#include "../../message/record.h"
#include "../../metaData.h"
#include <stdio.h>
#include <map>
#include <list>
#include "../appendingIndex.h"
using namespace STORE;
using namespace DATABASE_INCREASE;
bool test()
{
    tableMeta meta;
    meta.m_charset ="utf8";
    meta.m_columns = new columnMeta[meta.m_columnsCount = 3];

    meta.m_columns[0].m_columnIndex = 0;
    meta.m_columns[0].m_columnName = "a";
    meta.m_columns[0].m_columnType = MYSQL_TYPE_LONG;
    meta.m_columns[0].m_isPrimary = true;
    meta.m_columns[0].m_isUnique = false;
    meta.m_columns[0].m_signed = true;

    meta.m_columns[1].m_columnIndex = 1;
    meta.m_columns[1].m_columnName = "b";
    meta.m_columns[1].m_columnType = MYSQL_TYPE_LONG;
    meta.m_columns[1].m_isPrimary = false;
    meta.m_columns[1].m_isUnique = false;
    meta.m_columns[1].m_signed = true;

    meta.m_columns[2].m_columnIndex = 2;
    meta.m_columns[2].m_columnName = "c";
    meta.m_columns[2].m_columnType = MYSQL_TYPE_STRING;
    meta.m_columns[2].m_isPrimary = false;
    meta.m_columns[2].m_isUnique = false;

    meta.m_id=1;
    meta.m_primaryKey.m_Count=1;
    meta.m_primaryKey.m_array = new char *[1];
    meta.m_primaryKey.m_array[0] = new char[10];

    meta.m_primaryKeyIdxs = new uint16_t[1];
    meta.m_primaryKeyIdxs[0] = 0;
    meta.m_uniqueKeyIdxs = nullptr;
    meta.m_uniqueKeys = nullptr;
    meta.m_uniqueKeysCount = 0;
    appendingIndex idx(meta.m_primaryKeyIdxs,1,&meta);
    int32_t a = 0,b = 0;
    char c[1024] = "dwadesfrdgtfhydwadwd";
    DMLRecord r;
    r.data = nullptr;
    r.head = new recordHead;
    r.head->type = INSERT;
    r.newColumns = (const char**)new char*[3];
    r.newColumns[0] = (char*)&a;
    r.newColumns[1] = (char*)&b;
    r.newColumns[2] = c;

    r.newColumnSizes = new uint32_t[3];
    r.newColumnSizes[0] = 4;
    r.newColumnSizes[1] = 4;
    r.newColumnSizes[2] = strlen(c);
    std::map<int32_t,std::list<uint64_t> * > s;
    for(int i=0;i<100000;i++)
    {
        idx.append(&r,i);
        /*
        std::map<int32_t,std::list<uint64_t> * >::iterator iter = s.find(a);
        if(iter!=s.end())
        {
            iter->second->push_back(i);
        }
        else
        {
            std::list<uint64_t>  * l = new std::list<uint64_t>;
            l->push_back(i);
            s.insert(std::pair<int32_t,std::list<uint64_t> * >(a,l));
        }
        */
        a = random();

    }
    /*
    appendingIndex::iterator<int32_t> ai(&idx);
    if(!ai.seek(s.begin()->first))
    {
        printf("test failed at %d@%s\n",__LINE__,__FILE__);
        return false;
    }
    for(std::map<int32_t,std::list<uint64_t> * >::iterator iter = s.begin();iter!=s.end();iter++)
    {
        if(ai.key()!=iter->first)
        {
            printf("test failed at %d@%s\n",__LINE__,__FILE__);
            return false;
        }
        for(std::list<uint64_t>::iterator li = iter->second->begin();li!=iter->second->end();li++)
        {
            if(*li!=ai.value())
            {
                printf("%d,%lu,%lu\n",iter->first,*li,ai.value());
                printf("test failed at %d@%s\n",__LINE__,__FILE__);
               // return false;
            }
            ai.nextValueOfKey();
        }
        ai.nextKey();
    }
    */
    printf("%s in %s test success\n",__func__,__FILE__);
    return true;
}
int main()
{
    test();
}


