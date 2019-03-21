/*
 * appendingIndex.cpp
 *
 *  Created on: 2019年3月12日
 *      Author: liwei
 */
#include "appendingIndex.h"
namespace STORE{
    binaryType::binaryType():data(nullptr){

    }
    binaryType::binaryType(char* _data):data(_data){

    }
    binaryType::binaryType(const binaryType & dest):data(dest.data)
    {
    }
    binaryType binaryType::operator=(const binaryType & dest)
    {
        data = dest.data;
        return *this;
    }
    int binaryType::compare (const binaryType & dest) const
    {
        if(*(uint32_t*)data == *(uint32_t*)dest.data)
            return memcmp(data+4,dest.data+4,*(uint32_t*)data);
        else if(*(uint32_t*)data>*(uint32_t*)dest.data)
        {
            if(memcmp(data+4,dest.data+4,*(uint32_t*)data)>=0)
                return -1;
            else
                return 1;
        }
        else
        {
            if(memcmp(data+4,dest.data+4,*(uint32_t*)dest.data)>0)
                return 1;
            else
                return -1;
        }
    }
    bool binaryType::operator< (const binaryType & dest) const
    {
        return  compare(dest)<0;
    }
    bool binaryType::operator> (const binaryType & dest) const
    {
        return  compare(dest)>0;
    }
    void appendingIndex::appendMultiIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,keyChildInfo * child,uint64_t id,bool keyUpdated)
    {
        void ** keyArrays = index->m_arena->Allocate((index->m_columnCount-1)*sizeof(void*));
        if(keyUpdated)
        {
            for(uint16_t idx = 1;idx<index->m_columnCount;idx++)
                keyArrays[idx-1] = r->oldColumnOfUpdateType(index->m_columnIdxs[idx]);
        }
        else
        {
            for(uint16_t idx = 1;idx<index->m_columnCount;idx++)
                keyArrays[idx-1] = r->newColumns[index->m_columnIdxs[idx]];
        }

    }
    void appendingIndex::appendUint8Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<uint8_t> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(uint8_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(uint8_t*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(uint8_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(uint8_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }

    void appendingIndex::appendInt8Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<int8_t> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(int8_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(int8_t*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(int8_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(int8_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendUint16Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<uint16_t> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(uint16_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(uint16_t*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(uint16_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(uint16_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendInt16Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<int16_t> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(int16_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(int16_t*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(int16_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(int16_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendUint32Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<uint32_t> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(uint32_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(uint32_t*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(uint32_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(uint32_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendInt32Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<int32_t> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(int32_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(int32_t*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(int32_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(int32_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendUint64Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<uint64_t> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(uint64_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(uint64_t*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(uint64_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(uint64_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendInt64Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<int64_t> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(int64_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(int64_t*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(int64_t*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(int64_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendFloatIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<float> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(float*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(float*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(float*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(float*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendDoubleIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<double> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key = *(double*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(double*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(double*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(double*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendBinaryIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<binaryType> c;

        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            c.key.data = (char*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key.data = (char*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key.data = (char*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key.data = (char*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendUnionIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id)
    {
        KeyTemplate<unionKey> c;
        if(r->head->type == DATABASE_INCREASE::INSERT)
        {
            const char * k = unionKey::initKey(index->m_arena)
            c.key.key = (char*)r->newColumns[index->m_columnIdxs[0]];
            c.key.meta = r->newColumnSizes[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::DELETE)
        {
            c.key = *(float*)r->oldColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
        {
            c.key = *(float*)r->newColumns[index->m_columnIdxs[0]];
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(float*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
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

    void  appendingIndex::append(DATABASE_INCREASE::DMLRecord  * r,uint64_t id)
    {
        m_appendIndexFuncs[m_type](this,r,id);
    }
}



