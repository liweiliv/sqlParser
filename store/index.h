/*
 * index.h
 *
 *  Created on: 2019年1月7日
 *      Author: liwei
 */
#include <stdint.h>
#include <assert.h>
#include <string.h>
namespace STORE
{
#define SubIndexFixedFlag 0x00000001
struct subIndex{
    uint32_t count;
    uint32_t size;
    uint32_t flag;
};
enum SEARCH_TYPE{
    BEGIN,
    END,
    EQUAL,
    LESS,
    GREAT
};
struct subIndexFixed :public subIndex
{
    uint32_t fixedSize;
    uint8_t data[1];
    template<typename KEY,typename VALUE>
    struct iterator{
        uint32_t id;
        subIndexFixed *index;
        iterator(subIndexFixed * _index,SEARCH_TYPE type,const KEY & key = 0):index(_index)
        {
            assert(sizeof(KEY)==_index->fixedSize);
            if(count <= 0)
            {
                id = 0xffffffffu;
                return;
            }
            if(type == BEGIN)
                id = 0;
            else if(type == END)
                id = count-1;
            else
                search(type,key);
        }
        iterator(subIndexFixed * _index,uint32_t id):index(_index){
            if(index->count<id)
                this->id = 0xffffffffu;
            else
                this->id = id;
        }
        inline bool valid(){
            return id!=0xffffffffu;
        }
        inline KEY key(uint32_t idx)
        {
            return *(KEY*)(data+(sizeof(KEY)+sizeof(VALUE))*idx);
        }
        inline KEY key()
        {
            return *(KEY*)(data+(sizeof(KEY)+sizeof(VALUE))*id);
        }
        inline VALUE value(uint32_t idx)
        {
            return *(VALUE*)(data+(sizeof(KEY)+sizeof(VALUE))*idx+sizeof(KEY));
        }
        inline VALUE value()
        {
            return *(VALUE*)(data+(sizeof(KEY)+sizeof(VALUE))*id+sizeof(KEY));
        }
        inline void search(SEARCH_TYPE type,const KEY & key)
        {
            if(count<=0)
                id=0xffffffffu;
            else if(count<=8)
                sequentialSearch(type,key);
            else
                binarySearch(type,key);
        }
        inline void sequentialSearch(SEARCH_TYPE type,const KEY & key)
        {
            uint32_t idx = 0;
            KEY v ;
            while(idx<count)
            {
                v = key(idx);
                if(value>v)
                {
                    switch(type)
                    {
                    case LESS:
                        if(idx==0)
                            id = 0xffffffffu;
                        else
                            id = idx-1;
                        return;
                    case GREAT:
                        id = idx;
                        return;
                    default:
                        id = 0xffffffffu;
                        return;
                    }
                }
                else if(value==v)
                {
                    id = idx;
                    return;
                }
            }
            if(type==LESS)
                id = count-1;
            else
                id = 0xffffffffu;
        }
        inline void binarySearch(SEARCH_TYPE type,const KEY & key)
        {
            int32_t s=0,e= index->count-1,m;
            while(s<=e)
            {
                m = (s+e)>>1;
                KEY v = key(m);
                if(v<value)
                    s = m+1;
                else if(v>value)
                    e = m+1;
                else
                {
                    id =  m;
                    return;
                }
            }
            if(s>=count)
            {
                if(type==LESS)
                    id = count-1;
                else
                    id = 0xffffffffu;
                return;
            }
            if(e<0)
            {
                if(type==GREAT)
                    id = 0;
                else
                    id = 0xffffffffu;
                return;
            }
            switch(type)
            {
            case LESS:
                id =  s-1;
                return;
            case GREAT:
                id = s+1;
                return;
            default:
                id  =  0xffffffffu;
                return;
            }
        }
    };
};
struct subIndexVar :public subIndex
{
    uint32_t offsets[1];
    struct varData{
        char * data;
        uint32_t size;
        inline uint32_t compare(const varData & value)
        {
            uint32_t rtv = size-value.size;
            if(rtv!=0)
                return rtv;
            return memcmp(data,value.data,size);
        }
    };
    template<typename VALUE>
    struct iterator{
        uint32_t id;
        subIndexFixed *index;
        iterator(subIndexVar * _index,SEARCH_TYPE type,const varData & value = 0):index(_index)
        {
            if(index->count <= 0)
            {
                id = 0xffffffffu;
                return;
            }
            if(type == BEGIN)
                id = 0;
            else if(type == END)
                id = index->count-1;
            else
                search(type,value);
        }
        iterator(subIndexVar * _index,uint32_t id):index(_index){
            if(index->count<id)
                this->id = 0xffffffffu;
            else
                this->id = id;
        }
        inline bool valid(){
            return id!=0xffffffffu;
        }
        inline void key(uint32_t idx,varData &data)const
        {
            data.data = ((char*)&offsets[index->count+1])+offsets[idx];
            data.size = offsets[idx+1]-offsets[idx]-sizeof(VALUE);
        }
        inline void key(varData &data)const
        {
            data.data = ((char*)&offsets[index->count+1])+offsets[id];
            data.size = offsets[id+1]-offsets[id]-sizeof(VALUE);
        }
        inline const VALUE value(uint32_t idx)const
        {
            return *(VALUE*)( ((char*)&offsets[index->count+1]) +offsets[idx]+
                    offsets[idx+1]-offsets[idx]-sizeof(VALUE));
        }
        inline const VALUE value()const
        {
            return *(VALUE*)( ((char*)&offsets[index->count+1]) +offsets[id]+
                    offsets[id+1]-offsets[id]-sizeof(VALUE));
        }
        inline void search(SEARCH_TYPE type,const varData  & value)const
        {
            if(index->count<=0)
                id=0xffffffffu;
            else if(index->count<=8)
                sequentialSearch(type,value);
            else
                binarySearch(type,value);
        }
        inline void sequentialSearch(SEARCH_TYPE type,const varData & value)const
        {
            uint32_t idx = 0,cmp;
            varData v ;
            while(idx<index->count)
            {
                key(idx,v);
                cmp = v.compare(value);
                if(cmp>0)
                {
                    switch(type)
                    {
                    case LESS:
                        if(idx==0)
                            id = 0xffffffffu;
                        else
                            id = idx-1;
                        return;
                    case GREAT:
                        id = idx;
                        return;
                    default:
                        id = 0xffffffffu;
                        return;
                    }
                }
                else if(cmp==0)
                {
                    id = idx;
                    return;
                }
            }
            if(type==LESS)
                id = index->count-1;
            else
                id = 0xffffffffu;
        }
        inline void binarySearch(SEARCH_TYPE type,const varData & value) const
        {
            int32_t s=0,e= index->count-1,m,cmp;
            varData v;
            while(s<=e)
            {
                m = (s+e)>>1;
                key(m,v);
                cmp = v.compare(value);
                if(cmp<0)
                    s = m+1;
                else if(cmp>0)
                    e = m+1;
                else
                {
                    id =  m;
                    return;
                }
            }
            if(s>=index->count)
            {
                if(type==LESS)
                    id = index->count-1;
                else
                    id = 0xffffffffu;
                return;
            }
            if(e<0)
            {
                if(type==GREAT)
                    id = 0;
                else
                    id = 0xffffffffu;
                return;
            }
            switch(type)
            {
            case LESS:
                id =  s-1;
                return;
            case GREAT:
                id = s+1;
                return;
            default:
                id  =  0xffffffffu;
                return;
            }
        }
    };
};

struct Index{
    uint32_t size;
    uint32_t count;
    const subIndexFixed * tableIDs;
    const subIndexVar * indexs;
    Index(const char * data)
    {
        size = *(uint32_t*)data;
        count = *(uint32_t*)(data+sizeof(size));
        tableIDs = (const subIndexFixed*)(data+sizeof(size)+sizeof(count));
        assert(size>=sizeof(size)+sizeof(count)+tableIDs->size);
        indexs = (const subIndexVar * )(((const char*)tableIDs)+tableIDs->size);
        assert(size==sizeof(size)+sizeof(count)+tableIDs->size+indexs->size);
    }
    subIndex * get(uint64_t tableID)
    {
        subIndexFixed::iterator<uint64_t,uint32_t> iter(tableIDs,tableID,EQUAL);
        if(!iter.valid())
            return NULL;
        uint64_t idx = iter.value();
        subIndexVar::iterator<uint64_t> varIter(indexs,idx);
        if(!varIter.valid())
            return NULL;
        subIndexVar::varData data;
        varIter.key(data);
        return (subIndex*)data.data;
    }
};

}

