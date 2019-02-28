/*
 * block.h
 *
 *  Created on: 2019年1月18日
 *      Author: liwei
 */

#ifndef BLOCK_H_
#define BLOCK_H_
#include <stdint.h>
#include <atomic>
namespace STORE{
#define BLOCK_FLAG_APPENDING    0x01
#define BLOCK_FLAG_FINISHED     0x02
#define BLOCK_FLAG_FLUSHED      0x04
#define BLOCK_FLAG_HAS_REDO     0x08
#define BLOCK_FLAG_COMPRESS     0x10
#define BLOCK_FLAG_MULTI_TABLE  0x11
struct recordID{
    uint64_t id;
    uint32_t offset;
};
class block{
public:
    uint32_t m_flag;
    uint64_t m_blockID;
    uint64_t m_tableID;// if !(flag&BLOCK_FLAG_MULTI_TABLE)
    std::atomic<int> m_ref;
    uint64_t maxTime;
    uint64_t minTime;
    uint64_t maxDataIdx;
    uint64_t minDataIdx;
    inline bool use()
    {
        do{
            int ref = m_ref.load(std::memory_order_acquire);
            if(ref<0)
                return false;
            if(m_ref.compare_exchange_strong(ref,ref+1))
                break;
        }while(1);
        return true;
    }
    inline void unuse()
    {
        m_ref.fetch_sub(1);
    }
    virtual int flush() =0;
};
}



#endif /* BLOCK_H_ */
