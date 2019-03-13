/*
 * appendingIndex.cpp
 *
 *  Created on: 2019年3月12日
 *      Author: liwei
 */
#include "appendingIndex.h"
namespace STORE{
    binaryType::binaryType():size(0),data(nullptr){

    }
    binaryType::binaryType(uint32_t _size,char* _data):size(_size),data(_data){

    }
    binaryType::binaryType(const binaryType & dest)
    {
        size = dest.size;
        data = dest.data;
    }
    binaryType binaryType::operator=(const binaryType & dest)
    {
        size = dest.size;
        data = dest.data;
        return *this;
    }
    bool binaryType::operator< (const binaryType & dest) const
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
    bool binaryType::operator> (const binaryType & dest) const
    {
        if(size == dest.size)
            return memcmp(data,dest.data,size)<0;
        else if(size>dest.size)
        {
            if(memcmp(data,dest.data,size)<=0)
                return true;
            else
                return false;
        }
        else
        {
            if(memcmp(data,dest.data,size)<0)
                return true;
            else
                return false;
        }
    }
    void appendingIndex::appendMultiIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,keyChildInfo * child,uint64_t id)
    {

    }
    void appendingIndex::appendUint8Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<uint8_t> c;
        c.key = *(uint8_t*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }

    void appendingIndex::appendInt8Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<int8_t> c;
        c.key = *(int8_t*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendUint16Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<uint16_t> c;
        c.key = *(uint16_t*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendInt16Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<int16_t> c;
        c.key = *(int16_t*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendUint32Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<uint32_t> c;
        c.key = *(uint32_t*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendInt32Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<int32_t> c;
        c.key = *(int32_t*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendUint64Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<uint64_t> c;
        c.key = *(uint64_t*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendInt64Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<int64_t> c;
        c.key = *(int64_t*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendFloatIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<float> c;
        c.key = *(float*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendDoubleIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<double> c;
        c.key = *(double*)r->newColumns[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    void appendingIndex::appendBinaryIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<binaryType> c;
        c.key.data = (char*)r->newColumns[index->m_columnIdxs[0]];
        c.key.size = r->newColumnSizes[index->m_columnIdxs[0]];
        appendIndex(index,r,&c,id);
    }
    appendingIndex::appendingIndex(uint16_t *columnIdxs,uint16_t columnCount,tableMeta * meta,leveldb::Arena *arena ):m_columnIdxs(columnIdxs),m_columnCount(columnCount),m_meta(meta),m_arena(arena),m_localArena(arena!=nullptr)
    {
        if(m_arena == nullptr)
            m_arena = new leveldb::Arena();
        columnMeta * first = &meta->m_columns[columnIdxs[0]];
        switch(first->m_columnType)
        {
        case MYSQL_TYPE_TINY:
        {
            if(first->m_signed)
            {
                m_type = INT8;
                m_comp = new keyComparator<int8_t>;
                m_index = new leveldb::SkipList< KeyTemplate<int8_t>*,keyComparator<int8_t> >(*static_cast<keyComparator<int8_t>*>(m_comp),m_arena);
            }
            else
            {
                m_type = UINT8;
                m_comp = new keyComparator<uint8_t>;
                m_index = new leveldb::SkipList< KeyTemplate<uint8_t>*,keyComparator<uint8_t> >(*static_cast<keyComparator<uint8_t>*>(m_comp),m_arena);
            }
            break;
        }
        case MYSQL_TYPE_SHORT:
        {
            if(first->m_signed)
            {
                m_type = INT16;
                m_comp = new keyComparator<int16_t>;
                m_index = new leveldb::SkipList< KeyTemplate<int16_t>*,keyComparator<int16_t> >(*static_cast<keyComparator<int16_t>*>(m_comp),m_arena);
            }
            else
            {
                m_type = UINT16;
                m_comp = new keyComparator<uint16_t>;
                m_index = new leveldb::SkipList< KeyTemplate<uint16_t>*,keyComparator<uint16_t> >(*static_cast<keyComparator<uint16_t>*>(m_comp),m_arena);
            }
            break;
        }
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        {
            if(first->m_signed)
            {
                m_type = INT32;
                m_comp = new keyComparator<int32_t>;
                m_index = new leveldb::SkipList< KeyTemplate<int32_t>*,keyComparator<int32_t> >(*static_cast<keyComparator<int32_t>*>(m_comp),m_arena);
            }
            else
            {
                m_type = UINT32;
                m_comp = new keyComparator<uint32_t>;
                m_index = new leveldb::SkipList< KeyTemplate<uint32_t>*,keyComparator<uint32_t> >(*static_cast<keyComparator<uint32_t>*>(m_comp),m_arena);
            }
            break;
        }
        case MYSQL_TYPE_LONGLONG:
        {
            if(first->m_signed)
            {
                m_type = INT64;
                m_comp = new keyComparator<int64_t>;
                m_index = new leveldb::SkipList< KeyTemplate<int64_t>*,keyComparator<int64_t> >(*static_cast<keyComparator<int64_t>*>(m_comp),m_arena);
            }
            else
            {
                m_type = UINT64;
                m_comp = new keyComparator<uint64_t>;
                m_index = new leveldb::SkipList< KeyTemplate<uint64_t>*,keyComparator<uint64_t> >(*static_cast<keyComparator<uint64_t>*>(m_comp),m_arena);
            }
            break;
        }
        default:
        {
            m_comp = new keyComparator<binaryType>;
            m_index = new leveldb::SkipList< KeyTemplate<binaryType>*,keyComparator<binaryType> >(*static_cast<keyComparator<binaryType>*>(m_comp),m_arena);
            break;
        }
        }

    };
    appendingIndex::~appendingIndex()
    {
        if(m_index!=nullptr)
        {
            switch(m_type)
            {
            case INT8:
                delete static_cast<leveldb::SkipList< KeyTemplate<int8_t>*,keyComparator<int8_t> >*>(m_index);
                break;
            case UINT8:
                delete static_cast<leveldb::SkipList< KeyTemplate<uint8_t>*,keyComparator<uint8_t> >*>(m_index);
                break;
            case INT16:
                delete static_cast<leveldb::SkipList< KeyTemplate<int16_t>*,keyComparator<int16_t> >*>(m_index);
                break;
            case UINT16:
                delete static_cast<leveldb::SkipList< KeyTemplate<uint16_t>*,keyComparator<uint16_t> >*>(m_index);
                break;
            case INT32:
                delete static_cast<leveldb::SkipList< KeyTemplate<int32_t>*,keyComparator<int32_t> >*>(m_index);
                break;
            case UINT32:
                delete static_cast<leveldb::SkipList< KeyTemplate<uint32_t>*,keyComparator<uint32_t> >*>(m_index);
                break;
            case INT64:
                delete static_cast<leveldb::SkipList< KeyTemplate<int64_t>*,keyComparator<int64_t> >*>(m_index);
                break;
            case UINT64:
                delete static_cast<leveldb::SkipList< KeyTemplate<uint64_t>*,keyComparator<uint64_t> >*>(m_index);
                break;
            case FLOAT:
                delete static_cast<leveldb::SkipList< KeyTemplate<float>*,keyComparator<float> >*>(m_index);
                break;
            case DOUBLE:
                delete static_cast<leveldb::SkipList< KeyTemplate<double>*,keyComparator<double> >*>(m_index);
                break;
            case BINARY:
                delete static_cast<leveldb::SkipList< KeyTemplate<binaryType>*,keyComparator<binaryType> >*>(m_index);
                break;
            default:
                abort();
            }
        }
        if(m_localArena&&m_arena!=nullptr)
            delete m_arena;

    }
    typename appendingIndex::appendIndexFunc appendingIndex::m_appendIndexFuncs[] = {appendUint8Index,appendInt8Index,appendUint16Index,appendInt16Index,appendUint32Index,appendInt32Index,appendUint64Index,appendInt64Index,appendFloatIndex,appendDoubleIndex,appendBinaryIndex};

    int  appendingIndex::append(DATABASE_INCREASE::DMLRecord  * r,uint64_t id)
    {
        if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
           // if(TEST_BITMAP(r->updatedBitmap,m_columnIdxs[0]))
             //   m_appendIndexFuncs[m_type](this,r,id);
            m_appendIndexFuncs[m_type](this,r,id);
        }
        else if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            m_appendIndexFuncs[m_type](this,r,id);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            m_appendIndexFuncs[m_type](this,r,id);
        }
        else
        {
            return -1;
        }
        return 0;
    }
}



