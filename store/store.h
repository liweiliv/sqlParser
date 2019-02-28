/*
 * store.h
 *
 *  Created on: 2019年2月13日
 *      Author: liwei
 */

#ifndef STORE_H_
#define STORE_H_
#include "schedule.h"
#include "blockManager.h"
#include "config.h"
#include <glog/logging.h>
namespace STORE{
class record;
class store{
private:
    schedule m_schedule;
    blockManager m_blockManager;
    config * m_conf;
public:
    store(config * conf):m_schedule(conf),m_blockManager(conf),m_conf(conf)
    {

    }
    int start()
    {
        if(0!=m_schedule.start())
        {
            LOG(ERROR)<<"schedule module start failed";
            return -1;
        }
        if(0!=m_blockManager.start())
        {
            LOG(ERROR)<<"blockManager module start failed";
            m_schedule.stop();
            return -1;
        }
        return 0;
    }
    std::string updateConfig(const char *key,const char * value)
    {
        if(strncmp(key,C_SCHEDULE ".",sizeof(C_SCHEDULE))==0)
            return m_schedule.updateConfig(key,value);
        else if (strncmp(key,C_BLOCK_MANAGER ".",sizeof(C_BLOCK_MANAGER))==0)
            return m_blockManager.updateConfig(key,value);
        else
            return std::string("unknown config:")+key;
    }
};



}
#endif /* STORE_H_ */
