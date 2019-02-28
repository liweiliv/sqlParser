/*
 * heap.h
 *
 *  Created on: 2019年1月17日
 *      Author: liwei
 */

#ifndef HEAP_H_
#define HEAP_H_
#include <stdint.h>
template<typename T,typename Compare>
class heap{
private:
    uint32_t maxSize;
    uint32_t currentSize;
    T * h;
public:
    heap(uint32_t _heapSize):h(nullptr),maxSize(_heapSize),currentSize(0){
        h = new T[_heapSize];
    }
    ~heap()
    {
        delete []h;
    }
    inline uint32_t size()
    {
        return currentSize;
    }
    inline T get()
    {
        return h[0];
    }
};
template<typename T,typename Compare>
class minHeap :public heap<T,Compare>{
public:
    inline void sort()
    {

    }
    inline void insert(T data)
    {

    }
    inline void popMin()
    {

    }
    minHeap(uint32_t heapSize):heap<T,Compare>(heapSize)
    {
    }
    ~minHeap();
};




#endif /* HEAP_H_ */
