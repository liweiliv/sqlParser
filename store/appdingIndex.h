/*
 * appdingIndex.h
 *
 *  Created on: 2019年2月21日
 *      Author: liwei
 */

#ifndef APPDINGINDEX_H_
#define APPDINGINDEX_H_
#include <stdint.h>
#include "skiplist.h"
#include <string.h>
#include "metaData.h"
#include "record.h"
namespace STORE{
struct keyChildInfo{
    void ** subArray;
    uint32_t arraySize;
    uint32_t count;
};

template <typename T>
struct KeyTemplate
{
    T key;
    keyChildInfo child;
};
struct binarytype{
    uint32_t size;
    char * data;
    binarytype():size(0),data(nullptr){

    }
    binarytype(uint32_t _size,char* _data):size(_size),data(_data){

    }
    binarytype(const binarytype & dest)
    {
        size = dest.size;
        data = dest.data;
    }
    binarytype operator=(const binarytype & dest)
    {
        size = dest.size;
        data = dest.data;
        return *this;
    }
    bool operator> (const binarytype & dest)
    {
        if(size == dest.size)
            return memcmp(data,dest.data,size)>0;
        else if(size>dest.size)
        {
            if(memcmp(data,dest.data,size)>=0)
                return true;
            else
                return false;
        }
        else
        {
            if(memcmp(data,dest.data,size)>0)
                return true;
            else
                return false;
        }
    }
};
template <typename T>
struct keyComparator
{
    inline int operator()(const KeyTemplate<T> *& a, const KeyTemplate<T> *& b) const
    {
        if (a->key < b->key)
        {
            return -1;
        }
        else if (a->key > b->key)
        {
            return +1;
        }
        else
        {
            return 0;
        }
    }
};
class appdingIndex{
public:
    enum INDEX_TYPE{
        UINT8,
        INT8,
        UINT16,
        INT16,
        UINT32,
        INT32,
        UINT64,
        INT64,
        BINARY,
    };
private :
    INDEX_TYPE m_type;
    void * m_index;
    uint16_t *m_columnIdxs;
    uint16_t m_columnCount;
    tableMeta* m_meta;
    leveldb::Arena m_arena;
    typedef void (* appendIndexFunc) (appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id);
    static inline void appendUint8Index(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {

    }
    static inline void appendMultiIndex(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,keyChildInfo * child,uint64_t id)
    {

    }
    static inline void appendInt8Index(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {
        leveldb::SkipList< KeyTemplate<int8_t>*,keyComparator<int8_t> > * dates = static_cast<leveldb::SkipList< KeyTemplate<int8_t>*,keyComparator<int8_t> > *>(index);
        leveldb::SkipList< KeyTemplate<int8_t>*,keyComparator<int8_t> >::Iterator iter(dates);
        KeyTemplate<int8_t> c;
        c.key = (uint8_t)r->newColumns[index->m_columnIdxs[0]];
        KeyTemplate<int8_t> *k = nullptr;
        iter.Seek(&c);
        if(!iter.Valid())
        {
            KeyTemplate<int8_t> *k = (KeyTemplate<int8_t> *)index->m_arena.AllocateAligned(sizeof(KeyTemplate<int8_t>));
            k->key = c.key;
            k->child.subArray = (void**)index->m_arena.AllocateAligned(sizeof(void*)*(k->child.arraySize=5));
            k->child.count = 0;
            dates->Insert(k);
        }
        else
            k = (KeyTemplate<int8_t> *)iter.key();
        if(index->m_columnCount>0)
        {

        }
        else
        {
            if(k->child.count >=k->child.arraySize)
            {
                void ** tmp = (void**)index->m_arena.AllocateAligned(sizeof(void*)*(k->child.arraySize*=2));
                memcpy(tmp,k->child.subArray,sizeof(void*)*k->child.arraySize);
                k->child.subArray = tmp;
            }
            k->child.subArray[k->child.count] = (void*)id;
            __asm__ __volatile__("sfence" ::: "memory");
            k->child.count++;
        }
    }
    static inline void appendUint16Index(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {

    }
    static inline void appendInt16Index(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {

    }
    static inline void appendUint32Index(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {

    }
    static inline void appendInt32Index(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {

    }
    static inline void appendUint64Index(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {

    }
    static inline void appendInt64Index(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {

    }
    static inline void appendBinaryIndex(appdingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t size,uint64_t id)
    {

    }
    static appendIndexFunc m_appendIndexFuncs[] = {appendUint8Index,appendInt8Index,appendUint16Index,appendInt16Index,appendUint32Index,appendInt32Index,appendUint64Index,appendInt64Index,appendBinaryIndex};
public:
    appdingIndex(uint16_t *columnIdxs,uint16_t columnCount,tableMeta * meta):m_columnIdxs(columnIdxs),m_columnCount(columnCount),m_meta(meta)
    {

        columnMeta * first = meta->m_columns[columnIdxs[0]];
        switch(first->m_columnType)
        {
        case MYSQL_TYPE_TINY:
        {
            if(first->m_signed)
            {
                m_type = INT8;
                m_index = new leveldb::SkipList< KeyTemplate<int8_t>*,keyComparator<int8_t> >;
            }
            else
            {
                m_type = UINT8;
                m_index = new leveldb::SkipList< KeyTemplate<uint8_t>*,keyComparator<uint8_t> >;
            }
            break;
        }
        case MYSQL_TYPE_SHORT:
        {
            if(first->m_signed)
            {
                m_type = INT16;
                m_index = new leveldb::SkipList< KeyTemplate<int16_t>*,keyComparator<int16_t> >;
            }
            else
            {
                m_type = UINT16;
                m_index = new leveldb::SkipList< KeyTemplate<uint16_t>*,keyComparator<uint16_t> >;
            }
            break;
        }
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        {
            if(first->m_signed)
            {
                m_type = INT32;
                m_index = new leveldb::SkipList< KeyTemplate<int32_t>*,keyComparator<int32_t> >;
            }
            else
            {
                m_type = UINT32;
                m_index = new leveldb::SkipList< KeyTemplate<uint32_t>*,keyComparator<uint32_t> >;
            }
            break;
        }
        case MYSQL_TYPE_LONGLONG:
        {
            if(first->m_signed)
            {
                m_type = INT64;
                m_index = new leveldb::SkipList< KeyTemplate<int64_t>*,keyComparator<int64_t> >;
            }
            else
            {
                m_type = UINT64;
                m_index = new leveldb::SkipList< KeyTemplate<uint64_t>*,keyComparator<uint64_t> >;
            }
            break;
        }
        default:
        {
            m_index = new leveldb::SkipList< KeyTemplate<binarytype>*,keyComparator<binarytype> >;
            break;
        }
        }
    };
    int append(DATABASE_INCREASE::DMLRecord  * r,uint64_t id)
    {
        if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            if(TEST_BITMAP(r->updatedBitmap,m_columnIdxs[0]))
                m_appendIndexFuncs[m_type](this,r->oldColumns[m_columnIdxs[0]],r->oldColumnSizes[m_columnIdxs[0]] ,id);
            m_appendIndexFuncs[m_type](this,r->newColumns[m_columnIdxs[0]],r->oldColumnSizes[m_columnIdxs[0]] ,id);
        }
        else if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            m_appendIndexFuncs[m_type](this,r->newColumns[m_columnIdxs[0]],r->oldColumnSizes[m_columnIdxs[0]] ,id);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            m_appendIndexFuncs[m_type](this,r->oldColumns[m_columnIdxs[0]],r->oldColumnSizes[m_columnIdxs[0]] ,id);
        }
        else
        {
            return -1;
        }
        return 0;
    }
};
}



#endif /* APPDINGINDEX_H_ */
