/*
 * pageTable.h
 *
 *  Created on: 2019年1月22日
 *      Author: liwei
 */

#ifndef PAGETABLE_H_
#define PAGETABLE_H_
#define PT_HIGN_OFFSET 8
#define PT_HIGN_MASK 0xff000000
#define PT_MID_OFFSET 12
#define PT_MID_MASK  0x00fff000
#define PT_LOW_OFFSET 12
#define PT_LOW_MASK  0x00000fff

#define PT_HIGH(id) ((id)>>(PT_MID_OFFSET+PT_LOW_OFFSET))
#define PT_MID(id) (((id)&PT_MID_MASK)>>PT_LOW_OFFSET)
#define PT_LOW(id) ((id)&PT_LOW_MASK)
#include <stdint.h>
#include <atomic>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
class pageTable
{
private:
    struct lowNode
    {
        std::atomic<void*> child[PT_LOW(0xffffffffu)];
        lowNode()
        {
            for (uint16_t idx = 0; idx < PT_LOW(0xffffffffu); idx++)
                child[idx].store(nullptr, std::memory_order_relaxed);
        }

    };
    struct midNode
    {
        std::atomic<lowNode*> child[PT_MID(0xffffffffu)];
        midNode()
        {
            for (uint16_t idx = 0; idx < PT_MID(0xffffffffu); idx++)
                child[idx].store(nullptr, std::memory_order_relaxed);
        }
    };
    struct highNode
    {
        std::atomic<midNode*> child[PT_HIGH(0xffffffffu)];
        highNode()
        {
            for (uint16_t idx = 0; idx < PT_HIGH(0xffffffffu); idx++)
                child[idx].store(nullptr, std::memory_order_relaxed);
        }
    };
    highNode root;
    std::atomic<uint32_t> min;
    std::atomic<uint32_t> max;
    int (*destoryValueFunc)(void * data);
public:
    pageTable(int (*_destoryValueFunc)(void *)) :
            destoryValueFunc(_destoryValueFunc)
    {
        min.store(0, std::memory_order_relaxed);
        max.store(0, std::memory_order_relaxed);
    }
    ~pageTable()
    {
        clear();
    }
    void clear()
    {
        min.store(0, std::memory_order_relaxed);
        max.store(0, std::memory_order_relaxed);
        for (uint16_t i = 0; i < PT_HIGH(0xffffffffu); i++)
        {
            midNode * mid;
            if ((mid = root.child[i].load(std::memory_order_release)) != NULL)
            {
                for (uint16_t j = 0; j < PT_MID(0xffffffffu); j++)
                {
                    lowNode * low;
                    if ((low = mid->child[j].load(std::memory_order_release))
                            != NULL)
                    {
                        for (uint16_t m = 0; m < PT_LOW(0xffffffffu); m++)
                        {
                            void * data;
                            if ((data = low->child[m].load(
                                    std::memory_order_release)) != NULL)
                            {
                                if (destoryValueFunc)
                                    destoryValueFunc(data);
                            }
                        }
                        delete low;
                    }
                }
                delete mid;
            }
            else
                root.child[i].store(nullptr, std::memory_order_relaxed);
        }
    }
    inline const void * get(uint32_t id)
    {
        if (min.load(std::memory_order_release) > id
                || max.load(std::memory_order_release) < id)
            return nullptr;
        uint8_t highId = PT_HIGH(id);
        midNode * mid = root.child[highId].load(std::memory_order_release);
        if (mid == nullptr)
            return nullptr;
        uint16_t midId = PT_MID(id);
        lowNode * low = mid->child[midId].load(std::memory_order_release);
        if (low == nullptr)
            return nullptr;
        uint16_t lowId = PT_LOW(id);
        return low->child[lowId].load(std::memory_order_release);
    }
    inline const void * operator[](uint32_t id)
    {
        return get(id);
    }
    inline const void * begin()
    {
        do
        {
            void * data = nullptr;
            uint32_t id = min.load(std::memory_order_release);
            if (nullptr == (data = get(id)))
            {
                if (id == min.load(std::memory_order_release))
                    return nullptr;
            }
            else
                return data;
        } while (1);
    }
    inline const void * end()
    {
        do
        {
            void * data = nullptr;
            uint32_t id = max.load(std::memory_order_release);
            if (nullptr == (data = get(id)))
            {
                if (id == max.load(std::memory_order_release))
                    return nullptr;
            }
            else
                return data;
        } while (1);
    }
    inline void* set(uint32_t id, void * data)
    {
        uint8_t highId = PT_HIGH(id);
        midNode * mid = root.child[highId].load(std::memory_order_release);
        bool newMid = false, newLow = false;
        if (mid == nullptr)
        {
            mid = new midNode;
            newMid = true;
        }
        uint16_t midId = PT_MID(id);
        lowNode * low = mid->child[midId].load(std::memory_order_release);
        if (low == nullptr)
        {
            low = new lowNode;
            newLow = true;
        }
        uint16_t lowId = PT_LOW(id);
        void * nullData = nullptr;
        while (low->child[lowId].compare_exchange_weak(nullData, data,
                std::memory_order_acq_rel, std::memory_order_acq_rel))
        {
            if ((nullData = low->child[lowId].load(std::memory_order_release))
                    != nullptr)
                return nullData;
        }
        if (newLow)
        {
            lowNode * nullData = nullptr;
            while (mid->child[midId].compare_exchange_weak(nullData, low,
                    std::memory_order_acq_rel, std::memory_order_acq_rel))
            {
                if ((nullData = mid->child[midId].load(
                        std::memory_order_release)) != nullptr)
                    return nullData;
            }
        }
        if (newMid)
        {
            midNode * nullData = nullptr;
            while (root.child[midId].compare_exchange_weak(nullData, mid,
                    std::memory_order_acq_rel, std::memory_order_acq_rel))
            {
                if ((nullData = root.child[highId].load(
                        std::memory_order_release)) != nullptr)
                    return nullData;
            }
        }
        uint32_t _min;
        do
        {
            _min = min.load(std::memory_order_release);
            if (_min <= id)
                break;
            if (min.compare_exchange_weak(_min, id, std::memory_order_acq_rel,
                    std::memory_order_acq_rel))
                break;
        } while (1);
        uint32_t _max;
        do
        {
            _max = max.load(std::memory_order_release);
            if (_max >= id)
                break;
            if (max.compare_exchange_weak(_max, id, std::memory_order_acq_rel,
                    std::memory_order_acq_rel))
                break;
        } while (1);
        return low->child[lowId];
    }
    inline void erase(uint32_t id)
    {
        if (min.load(std::memory_order_release) > id
                || max.load(std::memory_order_release) < id)
            return;
        uint8_t highId = PT_HIGH(id);
        midNode * mid = root.child[highId].load(std::memory_order_release);
        if (mid == nullptr)
            return;
        uint16_t midId = PT_MID(id);
        lowNode * low = mid->child[midId].load(std::memory_order_release);
        if (low == nullptr)
            return;
        uint16_t lowId = PT_LOW(id);
        void * data = low->child[lowId].load(std::memory_order_release);
        if (data == nullptr)
            return;
        do
        {
            void *nulldata = nullptr;
            if (low->child[lowId].compare_exchange_weak(data, nulldata,
                    std::memory_order_acq_rel, std::memory_order_acq_rel))
                break;
            assert(low->child[lowId].load(std::memory_order_release) == data);
        } while (1);
        if (destoryValueFunc)
            destoryValueFunc(data);
    }
    inline void purge(uint32_t id)
    {
        if (min.load(std::memory_order_release) > id
                || max.load(std::memory_order_release) < id)
            return;
        uint32_t _min;
        do
        {
            _min = min.load(std::memory_order_release);
            if (_min <= id)
                break;
            if (min.compare_exchange_weak(_min, id, std::memory_order_acq_rel,
                    std::memory_order_acq_rel))
                break;
        } while (1);
        uint8_t highId = PT_HIGH(id);
        for (uint16_t i = 0; i < highId; i++)
        {
            midNode * mid;
            if ((mid = root.child[i].load(std::memory_order_release)) != NULL)
            {
                for (uint16_t j = 0; j < PT_MID(0xffffffffu); j++)
                {
                    lowNode * low;
                    if ((low = mid->child[j].load(std::memory_order_release))
                            != NULL)
                    {
                        for (uint16_t m = 0; m < PT_LOW(0xffffffffu); m++)
                        {
                            void * data;
                            if ((data = low->child[m].load(
                                    std::memory_order_release)) != NULL)
                            {
                                if (destoryValueFunc)
                                    destoryValueFunc(data);
                            }
                        }
                        delete low;
                    }
                }
                delete mid;
            }
            else
                root.child[i].store(nullptr, std::memory_order_relaxed);
        }
        midNode * mid = root.child[highId].load(std::memory_order_release);
        if (mid == nullptr)
            return;
        uint16_t midId = PT_MID(id);
        for (uint16_t j = 0; j < midId; j++)
        {
            lowNode * low;
            if ((low = mid->child[j].load(std::memory_order_release))
                    != NULL)
            {
                mid->child[j].store(nullptr,std::memory_order_release);
                for (uint16_t m = 0; m < PT_LOW(0xffffffffu); m++)
                {
                    void * data;
                    if ((data = low->child[m].load(
                            std::memory_order_release)) != NULL)
                    {
                        if (destoryValueFunc)
                            destoryValueFunc(data);
                    }
                }
                delete low;
            }
        }
        lowNode * low = mid->child[midId].load(std::memory_order_release);
        if (low == nullptr)
            return;
        uint16_t lowId = PT_LOW(id);
        for (uint16_t m = 0; m < lowId; m++)
        {
            void * data;
            if ((data = low->child[m].load(
                    std::memory_order_release)) != NULL)
            {
                low->child[m].store(nullptr,std::memory_order_release);
                if (destoryValueFunc)
                    destoryValueFunc(data);
            }
        }
    }
};

#endif /* PAGETABLE_H_ */
