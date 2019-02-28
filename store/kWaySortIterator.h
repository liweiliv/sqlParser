/*
 * kWaySortIterator.h
 *
 *  Created on: 2019年1月17日
 *      Author: liwei
 */

#ifndef KWAYSORTITERATOR_H_
#define KWAYSORTITERATOR_H_
#include "iterator.h"
#include "heap.h"
namespace STORE{

class kWaySortIterator :public iterator{
private:
    class Compare{
        static inline bool compare(const iterator * i,const iterator * j)
        {
            return m_less(i->value(),j->value());
        }
    };
    iterator ** m_iters;
    uint32_t m_iterCount;
    minHeap< iterator*,Compare > m_heap;
    iterator * m_current;
    bool (*m_less)(void *i,void*j);
public:
    kWaySortIterator(iterator ** iters,uint32_t iterCount,bool (*less)(void *i,void*j)):m_iters(iters),m_iterCount(iterCount),
    m_heap(0),m_current(nullptr),m_less(less){
        for(uint32_t idx = 0;idx<iterCount;idx++)
        {
            if(m_iters[idx]->valid())
                m_heap.insert(m_iters[idx]);
        }
        if(m_heap.size())
            m_current = m_heap.get();
    }
    inline bool valid()
    {
        return m_current!=nullptr&&m_current->valid();
    }
    inline status next()
    {
        if(m_heap.size()<=1)
            return false;
        if(m_current->next()==OK)
        {
            m_heap.popMin();
            m_heap.insert(m_current);
        }
        else
        {
            if(!m_current->end())
                return BLOCKED;
        }
        m_current = m_heap.get();
        return true;
    }
    inline void* value() const
    {
        return  m_current->value();
    }
    inline void wait()
    {
        m_current->wait();
    }
};
}




#endif /* KWAYSORTITERATOR_H_ */
