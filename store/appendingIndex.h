/*
 * appendingIndex.h
 *
 *  Created on: 2019年2月21日
 *      Author: liwei
 */

#ifndef APPENDINGINDEX_H_
#define APPENDINGINDEX_H_
#include <stdint.h>
#include "skiplist.h"
#include <string.h>
#include "metaData.h"
#include "record.h"
#include "iterator.h"
namespace STORE{

enum INDEX_TYPE{
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT,
    DOUBLE,
    BINARY,
    UNION,
    MAX_INDEX_TYPE
};
constexpr static uint8_t INDEX_TYPE_SIZE[] = {1,1,2,2,4,4,8,8,4,8,0};
struct keyChildInfo{
    uint64_t *subArray;
    uint32_t arraySize;
    uint32_t count;
};

template <typename T>
struct KeyTemplate
{
    T key;
    keyChildInfo child;
};
struct binaryType{
    char * data;
    binaryType();
    binaryType(char* _data);
    binaryType(const binaryType & dest);
    binaryType operator=(const binaryType & dest);
    bool operator< (const binaryType & dest) const;
    int compare (const binaryType & dest) const;
    bool operator> (const binaryType & dest) const;
};
struct unionKeyMeta{
     uint16_t m_keyCount;
     INDEX_TYPE *m_types;
 };
 struct unionKey{
     char * key;
     unionKeyMeta * meta;
     unionKey(const unionKey & dest);
     int compare (const unionKey & dest) const
     {
         assert(meta==dest.meta);
         const char * srcKey = key,*destKey = dest.key;
         for(uint16_t i=0;i<meta->m_keyCount;i++)
         {
             switch(meta->m_types[i])
             {
             case UINT8:
                 if(*(uint8_t*)srcKey!=*(uint8_t*)destKey)
                     return *(uint8_t*)srcKey-*(uint8_t*)destKey;
                 break;
             case INT8:
                 if(*(int8_t*)srcKey!=*(int8_t*)destKey)
                     return *(int8_t*)srcKey-*(int8_t*)destKey;
                 break;
             case UINT16:
                 if(*(uint16_t*)srcKey!=*(uint16_t*)destKey)
                     return *(uint16_t*)srcKey-*(uint16_t*)destKey;
                 break;
             case INT16:
                 if(*(int16_t*)srcKey!=*(int16_t*)destKey)
                     return *(int16_t*)srcKey-*(int16_t*)destKey;
                 break;
             case UINT32:
                 if(*(uint32_t*)srcKey!=*(uint32_t*)destKey)
                     return *(uint32_t*)srcKey-*(uint32_t*)destKey;
                 break;
             case INT32:
                 if(*(int32_t*)srcKey!=*(int32_t*)destKey)
                     return *(int32_t*)srcKey-*(int32_t*)destKey;
                 break;
             case UINT64:
                 if(*(uint64_t*)srcKey!=*(uint64_t*)destKey)
                     if(*(uint64_t*)srcKey>*(uint64_t*)destKey)
                         return 1;
                     else
                         return -1;
                 break;
             case INT64:
                 if(*(int64_t*)srcKey!=*(int64_t*)destKey)
                     if(*(int64_t*)srcKey>*(int64_t*)destKey)
                         return 1;
                     else
                         return -1;
                 break;
             case FLOAT:
                 if(*(float*)srcKey-*(float*)destKey>0.000001f||*(float*)srcKey-*(float*)destKey<-0.000001f)
                     if(*(float*)srcKey>*(float*)destKey)
                         return 1;
                     else
                         return -1;
                 break;
             case DOUBLE:
                 if(*(double*)srcKey-*(double*)destKey>0.000001f||*(double*)srcKey-*(double*)destKey<-0.000001f)
                     if(*(double*)srcKey>*(double*)destKey)
                         return 1;
                     else
                         return -1;
                 break;
             case BINARY:
             {
                 binaryType s(*(uint32_t*)srcKey,srcKey+4),d(*(uint32_t*)destKey,destKey+4);
                 int c = s.compare(d);
                 if(c!=0)
                     return c;
                 srcKey += 4+*(uint32_t*)srcKey;
                 destKey += 4+*(uint32_t*)destKey;
                 break;
             }
             default:
                 abort();
             }
             srcKey+= INDEX_TYPE_SIZE[meta->m_types[i]];
             destKey+= INDEX_TYPE_SIZE[meta->m_types[i]];
         }
         return 0;
     }
     bool operator> (const unionKey & dest) const;
     inline static const char* initKey(leveldb::Arena * arena,unionKeyMeta * keyMeta,uint16_t *columnIdxs,uint16_t columnCount,DATABASE_INCREASE::DMLRecord * r,bool keyUpdated = false)
      {
         uint32_t size = 0;
         if(r->head->type == DATABASE_INCREASE::INSERT)
         {
             for(uint16_t idx = 0;idx<keyMeta->m_keyCount;idx++)
             {
                 if(keyMeta->m_types[idx]!=UNION)
                     size+=INDEX_TYPE_SIZE[keyMeta->m_types[idx]];
                 else
                     size+=
             }
         }
         else if(r->head->type == DATABASE_INCREASE::DELETE)
         {

         }
         else if(r->head->type == DATABASE_INCREASE::UPDATE||r->head->type == DATABASE_INCREASE::REPLACE)
         {

         }
         else
             abort();
      }
 };
template <typename T>
struct keyComparator
{
    inline int operator()(const KeyTemplate<T> * a, const KeyTemplate<T> * b) const
    {
        if (a->key < b->key)
            return -1;
        else if (a->key > b->key)
            return +1;
        else
            return 0;
    }
};
template <>
struct keyComparator<double>
{
    inline int operator()(const KeyTemplate<double> * a, const KeyTemplate<double> * b) const
    {
        if (a->key < 0.000000001f+b->key)
            return -1;
        else if (a->key > b->key+0.000000001f)
            return +1;
        else
            return 0;
    }
};
template <>
struct keyComparator<float>
{
    inline int operator()(const KeyTemplate<float> * a, const KeyTemplate<float> * b) const
    {
        if (a->key < 0.000001f+b->key)
            return -1;
        else if (a->key > b->key+0.000001f)
            return +1;
        else
            return 0;
    }
};
class appendingIndex{
private :
    INDEX_TYPE m_type;
    void * m_index;
    uint16_t *m_columnIdxs;
    uint16_t m_columnCount;
    tableMeta* m_meta;
    leveldb::Arena *m_arena;
    bool m_localArena;
    void* m_comp;
    typedef void (* appendIndexFunc) (appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendMultiIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,keyChildInfo * child,uint64_t id,bool keyUpdated);
    template <typename T>
    static inline void appendIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,KeyTemplate<T> *c,uint64_t id,bool keyUpdated = false)
    {
        KeyTemplate<T> *k = nullptr;
        typename leveldb::SkipList< KeyTemplate<T>*,keyComparator<T> >::Iterator iter(static_cast<leveldb::SkipList< KeyTemplate<T>*,keyComparator<T> > * >(index->m_index));
        iter.Seek(c);
        if(!iter.Valid()||iter.key()->key>c->key)
        {
            k = (KeyTemplate<T> *)index->m_arena->AllocateAligned(sizeof(KeyTemplate<T>));
            k->key = c->key;
            k->child.subArray = (uint64_t*)index->m_arena->AllocateAligned(sizeof(uint64_t)*(k->child.arraySize=1));
            k->child.count = 0;
            static_cast<leveldb::SkipList< KeyTemplate<T>*,keyComparator<T> > *>(index->m_index)->Insert(k);
        }
        else
            k = (KeyTemplate<T> *)iter.key();
        if(index->m_columnCount>1)
        {
            appendMultiIndex(index,r,&k->child,id,keyUpdated);
        }
        else
        {
            if(k->child.count >=k->child.arraySize)
            {
                uint64_t * tmp = (uint64_t*)index->m_arena->AllocateAligned(sizeof(uint64_t)*(k->child.arraySize*2));
                memcpy(tmp,k->child.subArray,sizeof(uint64_t)*k->child.arraySize);
                k->child.arraySize*=2;
                k->child.subArray = tmp;
            }
            k->child.subArray[k->child.count] = id;
            __asm__ __volatile__("sfence" ::: "memory");
            k->child.count++;
        }

    }
    static inline void appendUint8Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendInt8Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendUint16Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendInt16Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendUint32Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendInt32Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendUint64Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendInt64Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendFloatIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendDoubleIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendBinaryIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
    static inline void appendUnionIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint64_t id);
public:
    static appendIndexFunc m_appendIndexFuncs[MAX_INDEX_TYPE];
public:
    appendingIndex(uint16_t *columnIdxs,uint16_t columnCount,tableMeta * meta,leveldb::Arena *arena = nullptr);
    ~appendingIndex();
    void append(DATABASE_INCREASE::DMLRecord  * r,uint64_t id);
    template <typename T>
    class iterator{
    private:
        appendingIndex * m_index;
        typename leveldb::SkipList< KeyTemplate<T>*,keyComparator<T> >::Iterator m_iter;
        uint32_t m_childIdx;
        const keyChildInfo * m_key;
    public:
        iterator(appendingIndex * index):m_index(index),m_iter(static_cast<leveldb::SkipList< KeyTemplate<T>*,keyComparator<T> > * >(index->m_index)),m_childIdx(0),m_key(nullptr)
        {
        }
       inline bool seek(const T & key)
        {
            KeyTemplate<T> k;
            k.key = key;
            m_iter.Seek(&k);
            if(!m_iter.Valid()||m_iter.key()->key>key)
                return false;
            if(0==m_iter.key()->child.count)
                return false;
            m_key = &m_iter.key()->child;
            m_childIdx = 0;
            return true;
        }
       inline bool nextKey()
        {
            m_childIdx = 0;
            m_iter.Next();
            if(!m_iter.Valid())
                return false;
            m_key = &m_iter.key()->child;
            return true;
        }
       inline bool nextValueOfKey()
        {
            if(m_childIdx>=m_key->count)
                return false;
            else
                m_childIdx++;
            return true;
        }
        inline uint64_t value() const
        {
            return m_key->subArray[m_childIdx];
        }
        inline const T & key() const
        {
            return  m_iter.key()->key;
        }
    };
};
}
#endif /* APPENDINGINDEX_H_ */
