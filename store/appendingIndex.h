/*
 * appendingIndex.h
 *
 *  Created on: 2019年2月21日
 *      Author: liwei
 */

#ifndef APPENDINGINDEX_H_
#define APPENDINGINDEX_H_
#include <stdint.h>
#include <string.h>
#include "../util/skiplist.h"
#include "../metaData.h"
#include "../message/record.h"
#include "iterator.h"
#include "columnType.h"
namespace STORE{
struct keyChildInfo{
    uint32_t *subArray;
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
    const char * data;
    binaryType();
    binaryType(const char* _data);
    binaryType(const binaryType & dest);
    binaryType operator=(const binaryType & dest);
    bool operator< (const binaryType & dest) const;
    int compare (const binaryType & dest) const;
    bool operator> (const binaryType & dest) const;
};
struct unionKeyMeta{
     uint16_t m_keyCount;
     uint8_t *m_types;
	 unionKeyMeta() :m_keyCount(0), m_types(nullptr) {}
	 unionKeyMeta(const uint16_t *columnIndexs,uint16_t columnCount,tableMeta* meta) :m_keyCount(0), m_types(nullptr) {
		 init(columnIndexs, columnCount, meta);
	 }
	 bool init(const uint16_t *columnIndexs, uint16_t columnCount, tableMeta* meta) {
		 if (m_types)
			 delete[]m_types;
		 m_types = new uint8_t[columnCount];
		 for (uint16_t i = 0; i < columnCount; i++)
		 {
			 m_types[i] = meta->getColumn(columnIndexs[i])->m_columnType;
			 if (!columnInfos[m_types[i]].asIndex)
			 {
				 delete[]m_types;
				 m_types = nullptr;
				 return false;
			 }
		 }
		 m_keyCount = columnCount;
	 }
	 ~unionKeyMeta() {
		 if (m_types != nullptr)
			 delete m_types;
	 }
 };
 struct unionKey{
     const char * key;
     const unionKeyMeta * meta;
	 unionKey();
	 unionKey(const unionKey & dest);
	 int compare(const unionKey & dest) const;
	 inline bool operator> (const unionKey & dest) const;
	 inline static const char* initKey(leveldb::Arena * arena, unionKeyMeta * keyMeta, uint16_t *columnIdxs, uint16_t columnCount, DATABASE_INCREASE::DMLRecord * r, bool keyUpdated = false);
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
    uint8_t m_type;
    void * m_index;
    uint16_t *m_columnIdxs;
    uint16_t m_columnCount;
    tableMeta* m_meta;
	unionKeyMeta  m_ukMeta;
    leveldb::Arena *m_arena;
    bool m_localArena;
    void* m_comp;
    typedef void (* appendIndexFunc) (appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    template <typename T>
    static inline void appendIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,KeyTemplate<T> *c,uint32_t id,bool keyUpdated = false)
    {
        KeyTemplate<T> *k = nullptr;
        typename leveldb::SkipList< KeyTemplate<T>*,keyComparator<T> >::Iterator iter(static_cast<leveldb::SkipList< KeyTemplate<T>*,keyComparator<T> > * >(index->m_index));
        iter.Seek(c);
        if(!iter.Valid()||iter.key()->key>c->key)
        {
            k = (KeyTemplate<T> *)index->m_arena->AllocateAligned(sizeof(KeyTemplate<T>));
            k->key = c->key;
            k->child.subArray = (uint32_t*)index->m_arena->AllocateAligned(sizeof(uint32_t)*(k->child.arraySize=1));
            k->child.count = 0;
            static_cast<leveldb::SkipList< KeyTemplate<T>*,keyComparator<T> > *>(index->m_index)->Insert(k);
        }
        else
            k = (KeyTemplate<T> *)iter.key();
		if (k->child.count >= k->child.arraySize)
		{
			uint32_t * tmp = (uint32_t*)index->m_arena->AllocateAligned(sizeof(uint32_t)*(k->child.arraySize * 2));
			memcpy(tmp, k->child.subArray, sizeof(uint32_t)*k->child.arraySize);
			k->child.arraySize *= 2;
			k->child.subArray = tmp;
		}
		k->child.subArray[k->child.count] = id;
		__asm__ __volatile__("sfence" ::: "memory");
		k->child.count++;
    }
    static inline void appendUint8Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendInt8Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendUint16Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendInt16Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendUint32Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendInt32Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendUint64Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendInt64Index(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendFloatIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendDoubleIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendBinaryIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
    static inline void appendUnionIndex(appendingIndex * index,DATABASE_INCREASE::DMLRecord * r,uint32_t id);
public:
    static appendIndexFunc m_appendIndexFuncs[];
public:
    appendingIndex(uint16_t *columnIdxs,uint16_t columnCount,tableMeta * meta,leveldb::Arena *arena = nullptr);
    ~appendingIndex();
    void append(DATABASE_INCREASE::DMLRecord  * r,uint32_t id);
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
        inline uint32_t value() const
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
