/*
 * unblockedQueue.h
 *
 *  Created on: 2019年1月14日
 *      Author: liwei
 */

#ifndef UNBLOCKEDQUEUE_H_
#define UNBLOCKEDQUEUE_H_
#include <atomic>
#include <stdlib.h>
#include <string.h>
class mempool{
    uint32_t m_size;
public:
    mempool(uint32_t size):m_size(size){}
    ~mempool(){}
    virtual void * allocData()
    {
        return malloc(m_size);
    }
    virtual void freeBack(void * m)
    {
        free(m);
    }
};
#define BSSQueueNodeBufSize 256
template<typename T>
struct unblockedQueueNode
{
    T buf[BSSQueueNodeBufSize/sizeof(T)];
    std::atomic<unblockedQueueNode> * next;
    std::atomic<uint8_t> start;
    std::atomic<uint8_t> end;
    static unblockedQueueNode *createNewNode(mempool * pool)
    {
        unblockedQueueNode * node = static_cast<unblockedQueueNode*>(pool->allocData());
        memset(node->buf,0,sizeof(buf));
        node->next->store(NULL,std::memory_order_relaxed);
        start.store(0,std::memory_order_relaxed);
        end.store(0,std::memory_order_relaxed);
        return node;
    }
};
template<typename T>
struct unblockedQueue
{
    std::atomic< unblockedQueueNode<T> * > _head;
    std::atomic< unblockedQueueNode<T> * > _end;
    void * userData;
    mempool * pool;

    inline void init(mempool * pool,void * userData)
    {
        this->pool = pool;
        this->userData = userData;
        _head.store(unblockedQueueNode<T>::createNewNode(pool),std::memory_order_relaxed);
        _end.store(_head.load(std::memory_order_relaxed),std::memory_order_relaxed);
        _head.load(std::memory_order_relaxed)->next->store(_end,std::memory_order_relaxed);
    }
    inline void push(const T & value)
    {
        unblockedQueueNode<T> * end = _end.load(std::memory_order_relaxed);
        if(end->end == BSSQueueNodeBufSize/sizeof(T)-1)
        {
            unblockedQueueNode<T> * tmp = unblockedQueueNode<T>::createNewNode(pool);
            tmp->buf[0] = value;
            tmp->end.store(1,std::memory_order_relaxed);
            end->next->store(tmp,std::memory_order_acq_rel);
            _end.store(tmp,std::memory_order_acq_rel);
        }
        else
        {
            end->buf[end->end.load(std::memory_order_relaxed)] = value;
            end->end.fetch_add(1,std::memory_order_acq_rel);
        }
    }
    inline const T  head_PopOnlyThread()
    {
        unblockedQueueNode<T> * head = _head.load(std::memory_order_relaxed);
        uint8_t start = head->start.load(std::memory_order_relaxed);
        if(start!=head->end.load(std::memory_order_acquire))
            return head->buf[start];
        if(start!=BSSQueueNodeBufSize/sizeof(T)-1)
        {
            if(start==head->end.load(std::memory_order_acquire))
                return NULL;
            else
                return head->buf[start];
        }
        else
        {
            if(head->next->load(std::memory_order_acquire)!=NULL)
            {
                return head->next->load(std::memory_order_relaxed).buf[0];
            }
            else
                return NULL;
        }
    }
    inline T pop()
    {
        T data;
        unblockedQueueNode<T> * head = _head.load(std::memory_order_relaxed);
        uint8_t start = head->start.load(std::memory_order_relaxed);
        if(start!=head->end.load(std::memory_order_acquire))
        {
            data = head->buf[head->start.load(std::memory_order_relaxed)];
            head->start.fetch_add(1,std::memory_order_release);
            return data;
        }
        if(start!=BSSQueueNodeBufSize/sizeof(T)-1)
        {
            if(start==head->end.load(std::memory_order_acquire))
                return NULL;
            else
            {
                data = head->buf[head->start.load(std::memory_order_relaxed)];
                head->start.fetch_add(1,std::memory_order_release);
                return data;
            }
        }
        else
        {
            if(head->next->load(std::memory_order_acquire)!=NULL)
            {
                data = head->next->buf[0];
                head->next->load(std::memory_order_relaxed).start.store(1,std::memory_order_release);
                _head.store(head->next->load(std::memory_order_relaxed),std::memory_order_seq_cst);
                pool->freeBack(head);
                return data;
            }
            else
                return NULL;
        }
    }

};




#endif /* UNBLOCKEDQUEUE_H_ */
