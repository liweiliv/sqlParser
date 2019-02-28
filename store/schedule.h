/*
 * cond.h
 *
 *  Created on: 2019年1月14日
 *      Author: liwei
 */

#ifndef SCHEDULE_H_
#define SCHEDULE_H_
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "unblockedQueue.h"
#include <atomic>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include "iterator.h"
#include "config.h"
#include <glog/logging.h>

namespace STORE{
#define COND_LEVEL 5
#define C_SCHEDULE "schedule"
#define C_MAX_WORKERS C_SCHEDULE ".maxWorders"
#define C_MAX_IDLE_ROUND C_SCHEDULE ".maxIdleRound"
class schedule
{
public:
    struct handle{
        iterator * iter;
        schedule * sc;
        void * data;
        void (*workFunc)(handle * h);
        void (*wait)(handle * h);
        void (*destroy)(handle * h);
        void (*finish)(handle * h);
    };
private:
    bool m_running;
    pthread_t m_workers[256];
    pthread_t m_schedule;
    std::atomic<uint16_t> m_workerCount;
    uint16_t m_maxWorders;
    unblockedQueue<handle *> m_activeHandles;
    int m_maxIdleRound;
    config * m_config;
public:
    schedule(config * conf):m_running(false),m_schedule(0),m_config(conf)
    {
        memset(m_workers,0,sizeof(m_workers));
        m_maxWorders = atoi(m_config->get("store",C_MAX_WORKERS,"8").c_str());
        if(m_maxWorders>sizeof(m_workers)/sizeof(pthread_t))
            m_maxWorders = sizeof(m_workers)/sizeof(pthread_t);
        m_maxIdleRound = atoi(m_config->get("store",C_MAX_IDLE_ROUND,"100").c_str());
        m_workerCount.store(0);
    }
    std::string updateConfig(const char *key,const char * value)
    {
        if(strcmp(key,C_MAX_WORKERS)==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if(value[idx]>'9'||value[idx]<'0')
                    return "value of config :maxWorders must be a number";
            }
            if(atol(value)>sizeof(m_workers)/sizeof(pthread_t))
            {
                char numBuf[32] = {0};
                sprintf(numBuf,"%d",sizeof(m_workers)/sizeof(pthread_t));
                return std::string("value of config :maxWorders is too large,max is ")+numBuf;
            }
            if(atol(value)<=0)
            {
                return std::string("value of config :maxWorders must greater than 0 ");
            }
            m_maxWorders  = atoi(value);
        }
        else if(strcmp(key,C_MAX_IDLE_ROUND)==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if(value[idx]>'9'||value[idx]<'0')
                    return "value of config :maxIdleRound must be a number";
            }
            m_maxIdleRound  = atoi(value);
        }
        else
            return std::string("unknown config:")+key;
        m_config->set("store",key,value);
        return std::string("update config:")+key+" success";
    }
    inline void wake(handle * h)
    {
        m_activeHandles.push(h);
    }
    int start()
    {
        m_running = true;
        LOG(INFO)<<"schedule starting...";
        if(0!=pthread_create(&m_workers[0],nullptr,worker,(void*)this))
        {
            m_running = false;
            LOG(ERROR)<<"schedule create wroker thread failed";
            return -1;
        }
        if(0!=pthread_create(&m_schedule,nullptr,scheduling,(void*)this))
        {
            LOG(ERROR)<<"schedule create scheduling thread failed";
            m_running = false;
            pthread_join(m_workers[0],nullptr);
            m_workers[0] = 0;
            return -1;
        }
        LOG(INFO)<<"schedule started";
        return 0;
    }
    int stop()
    {
        LOG(INFO)<<"schedule stoping...";
        m_running =  false;
        if(m_schedule!=0)
        {
            pthread_join(m_schedule,nullptr);
            m_schedule = 0;
        }
        for(int i = 0;i<sizeof(m_workers)/sizeof(pthread_t);i++)
        {
            if(m_workers[i]!=0)
            {
                pthread_join(m_workers[i],nullptr);
                m_workers[i] = 0;
            }
        }
        LOG(INFO)<<"schedule stopped...";
        return 0;
    }
private:
    inline handle * getNextActiveHandle()
    {
        return m_activeHandles.pop();
    }
    static void worker(void *argv)
    {
        schedule * s = static_cast<schedule*>(argv);
        clock_t c = clock();
        uint8_t idleRound = 0;
        uint16_t workerCount = 0;
        s->m_workerCount++;
        while(s->m_running)
        {
            handle * h = getNextActiveHandle();
            if(h == NULL)
            {
                if(++idleRound >= s->m_maxIdleRound&&(workerCount = s->m_workerCount.load(std::memory_order_relaxed))>1)
                {
                    if(s->m_workerCount.compare_exchange_strong(workerCount,workerCount-1))
                        break;
                }
                usleep(1000);
                continue;
            }
            else
                idleRound = 0;
            iterator::status nextStatus;
            clock_t now;
PROCESS:
            h->workFunc(h);
            now = clock();
            nextStatus=h->iter->next();
            switch(nextStatus)
            {
            case iterator::OK:
                if(s->m_running&&(now <c||now >=c+CLOCKS_PER_SEC/1000))
                {
                    c = now;
                    break;
                }
                else
                    goto PROCESS;
            case iterator::ENDED:
                h->finish(h);
                break;
            case iterator::INVALID:
                h->destroy(h);
                delete h;
                break;
            case iterator::BLOCKED:
                h->wait(h);
                break;
            }
            if(!s->m_running)
                break;
            if((workerCount = s->m_workerCount.load(std::memory_order_relaxed))>s->m_maxWorders)
            {
                if(s->m_workerCount.compare_exchange_strong(workerCount,workerCount-1))
                        break;
            }
        }
        pthread_exit((void*)1ul);
    }
    static void scheduling(void *argv)
    {
        schedule * s = static_cast<schedule*>(argv);
        int busy = 0;
        while(s->m_running)
        {
            if(m_activeHandles._end!=m_activeHandles._head)
            {
                if(++busy>1024&&m_workerCount.load(std::memory_order_relaxed)<m_maxWorders)
                {
                    int idx = 0;
                    for(;idx<m_maxWorders;idx++)
                    {
                        if(m_workers[idx]!=0)
                        {
                            void* rtv;
                            if(0==pthread_tryjoin_np(m_workers[idx],&rtv))//todo
                            {
                                m_workers[idx] = 0;
                                break;
                            }

                        }
                        else
                            break;
                    }
                    pthread_create(&m_workers[idx],nullptr,worker,(void*)this);
                }
            }
            else
            {
                if((busy-=10)<0)
                    busy = 0;
            }
            usleep(100000u);

        }

    }
};
}



#endif /* SCHEDULE_H_ */
