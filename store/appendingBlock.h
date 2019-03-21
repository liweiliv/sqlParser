/*
 * appendingBlock.cpp
 *
 *  Created on: 2019年1月7日
 *      Author: liwei
 */
#include <atomic>
#include <time.h>
#include "fileOpt.h"
#include "skiplist.h"
#include "metaDataCollection.h"
#include <unorderMapUtil.h>
#include "record.h"
#include "filter.h"
#include "iterator.h"
#include "schedule.h"
#include "block.h"
#include <string.h>
#include <glog/logging.h>
#include "metaData.h"
namespace STORE
{

class appendingBlock: public block
{
public:
    enum appendingBlockStaus
    {
        OK, FULL, ILLEGAL, FAULT
    };
private:
    struct tableIndexInfo
    {
        tableMeta *meta;
        void ** indexs;
        uint8_t indexCount;
        tableIndexInfo(tableMeta *_meta):meta(_meta),indexs(nullptr),indexCount(0)
        {
            if(meta!=nullptr)
            {
                indexCount = meta->m_uniqueKeysCount+(meta->m_primaryKey.m_array!=nullptr)?1:0;
                indexs = (void**)malloc(sizeof(void*)*indexCount);
            }
        }
    };
    typedef std::tr1::unordered_map<uint64_t,tableIndexInfo*> tableIndexMap;

    std::atomic<uint32_t> m_ref;
    tableIndexMap m_tables;
    char * m_buf;
    recordID * m_recordIDs;
    uint64_t m_startID;
    uint64_t m_endID;
    uint32_t m_bufSize;
    std::atomic<uint32_t> m_offset;
    std::atomic<bool> m_ended;
    std::string m_path;
    appendingBlockStaus m_status;
    int m_fd;
    int m_redoFd;

    int32_t m_redoUnflushDataSize;
    int32_t m_redoFlushDataSize;
    int32_t m_redoFlushPeriod; //micro second
    struct timespec m_lastFLushTime;
public:
    appendingBlock(uint32_t flag, const char * logDir, const char * logPrefix,
            uint32_t bufSize,int32_t redoFlushDataSize,int32_t redoFlushPeriod,uint64_t startID) :
                m_startID(startID),m_fd(0),m_endID(startID),m_bufSize(bufSize), m_status(OK), m_redoFd(-1),m_redoUnflushDataSize(0),m_redoFlushDataSize(redoFlushDataSize),
            m_redoFlushPeriod(redoFlushPeriod)
    {
        char fileName[256];
        sprintf(fileName, "%s/%s.%lu", logDir, logPrefix, m_blockID);
        m_path.assign(fileName);
        m_buf = (char*) malloc(bufSize);
        m_recordIDs = (recordID*)malloc(sizeof(recordID)*1024*128);
    }
    int openRedoFile()
    {
        if (0
                > (m_redoFd = open((m_path + ".redo").c_str(), O_RDWR | O_CREAT,
                        S_IRUSR | S_IWUSR | S_IRGRP)))
        {
            LOG(ERROR)<<"open redo file :"<<m_path<<".redo failed for errno:"<<errno<<",error info:"<<strerror(errno);
            return -1;
        }
        int fileSize;
        if((fileSize = lseek(m_redoFd,0,SEEK_END))<m_offset.load(std::memory_order_relaxed))
        {
            LOG(ERROR)<<"eopen redo file :"<<m_path<<".redo failed for file size check failed,expect file size:"
            <<m_offset.load(std::memory_order_relaxed)<<",actually is "<<fileSize;
            return -1;
        }
        if(fileSize!=m_offset.load(std::memory_order_relaxed))
        {
            if(0!=ftruncate(m_redoFd,m_offset.load(std::memory_order_relaxed)))
            {
                LOG(ERROR)<<"ftruncate redo file :"<<m_path<<".redo to size:"<<m_offset.load(std::memory_order_relaxed)<<"failed for ferrno:"<<errno<<",error info:"<<strerror(errno);
                return -1;
            }
            if(lseek(m_redoFd,m_offset.load(std::memory_order_relaxed),SEEK_SET)!=m_offset.load(std::memory_order_relaxed))
            {
                LOG(ERROR)<<"open redo file :"<<m_path<<".redo failed for file size check failed,expect file size:"
                <<m_offset.load(std::memory_order_relaxed)<<",actually is "<<fileSize;
                return -1;
            }
        }
        return 0;
    }
    int recoveryFromRedo()
    {
        if (m_redoFd >0)
            close(m_redoFd);
        if(0>(m_redoFd = open((m_path+".redo").c_str(),O_RDWR)))
        {
            LOG(ERROR)<<"open redo file :"<<m_path<<".redo failed for errno:"<<errno<<",error info:"<<strerror(errno);
            return -1;
        }
        int size = lseek(m_redoFd,0,SEEK_END);//get fileSize
        if(size<0)
        {
            LOG(ERROR)<<"get size of  redo file :"<<m_path<<".redo failed for errno:"<<errno<<",error info:"<<strerror(errno);
            return -1;
        }
        if(size == 0) //empty file
            return 0;
        char * buf = (char*)malloc(size);
        if(buf == NULL)
        {
            LOG(ERROR)<<"alloc "<<size<<" byte memory failed";
            return -1;
        }
        if(0!=lseek(m_redoFd,0,SEEK_SET))//seek to begin of file
        {
            free(buf);
            LOG(ERROR)<<"leeek to begin of file :"<<m_path<<".redo failed for errno:"<<errno<<",error info:"<<strerror(errno);
            return -1;
        }
        if(size!=fileRead(m_redoFd,(uint8_t*)buf,size))//read all data one time
        {
            free(buf);
            LOG(ERROR)<<"read redo file :"<<m_path<<".redo failed for errno:"<<errno<<",error info:"<<strerror(errno);
            return -1;
        }
        m_flag &= ~BLOCK_FLAG_HAS_REDO;//unset BLOCK_FLAG_HAS_REDO,so call [append] will not write redo file
        DATABASE_INCREASE::recordHead* head = (DATABASE_INCREASE::recordHead*) buf;
        while((char*)head<=buf+size)
        {
            if(((char*)head)+sizeof(DATABASE_INCREASE::recordHead)>buf+size||((char*)head)+head->size>buf+size)//unfinished write ,truncate
            {
                LOG(WARNING)<<"get an incomplete redo data in file:"<<m_path<<".redo ,offset is "<<((char*)head)-buf;
                close(m_redoFd);
                if(0!=openRedoFile())//openRedoFile will truncate file
                {
                    LOG(WARNING)<<"reopen redo file:"<<m_path<<".redo failed";
                    free(buf);
                    m_flag |= BLOCK_FLAG_HAS_REDO; //reset BLOCK_FLAG_HAS_REDO
                    return -1;
                }
                break;
            }
            if(append((char*)head)!=OK)
            {
                LOG(ERROR)<<"recoveryFromRedo from  file :"<<m_path<<".redo failed for append data failed";
                free(buf);
                m_flag |= BLOCK_FLAG_HAS_REDO; //reset BLOCK_FLAG_HAS_REDO
                return -1;
            }
            ((char*)head)+=head->size;
        }
        m_flag |= BLOCK_FLAG_HAS_REDO; //reset BLOCK_FLAG_HAS_REDO
        LOG(INFO)<<"recoveryFromRedo from  file :"<<m_path<<".redo success";
        free(buf);
        return 0;
    }
    appendingBlockStaus writeRedo(const char * data)
    {
        if (m_redoFd < 0&&0!=openRedoFile())
            return FAULT;
        DATABASE_INCREASE::recordHead* head = (DATABASE_INCREASE::recordHead*) data;
        int writeSize;
        if(head->size!=(writeSize=fileWrite(m_redoFd,(uint8_t*)data,head->size)))
        {
            if(errno==EBADF) //maybe out time or other cause,reopen it
            {
                LOG(WARNING)<<"write redo file:"<<m_path<<".redo failed for "<<strerror(errno)<<" reopen it";
                if (0!=openRedoFile())
                    return FAULT;
                return writeRedo(data);
            }
            else
            {
                LOG(ERROR)<<"write redo file :"<<m_path<<".redo failed for errno:"<<errno<<",error info:"<<strerror(errno);
                return FAULT;
            }
        }
        struct timespec now;
        if(m_redoFlushDataSize==0||//m_redoFlushDataSize == 0 means flush immediately
                m_redoFlushPeriod==0|| //m_redoFlushPeriod == 0 also means flush immediately
                (m_redoFlushDataSize>0&&(m_redoUnflushDataSize+=head->size)>=m_redoFlushDataSize)||//check if unflushed data big enough
                (m_redoFlushPeriod>0&& //check if time from last flush is long enough
                        (clock_gettime(CLOCK_REALTIME,&now),
                                1000000000*(now.tv_sec-m_lastFLushTime.tv_sec-1) +now.tv_nsec+1000000000 -m_lastFLushTime.tv_nsec)>=m_redoFlushPeriod*1000000))
        {
            if(0!=fsync(m_redoFd))
            {
                LOG(ERROR)<<"fsync redo file :"<<m_path<<".redo failed for errno:"<<errno<<",error info:"<<strerror(errno);
                return FAULT;
            }
            m_redoUnflushDataSize = 0;
            clock_gettime(CLOCK_REALTIME,&m_lastFLushTime);
            return OK;
        }
        return OK;
    }


    appendingBlockStaus append(const char * data)
    {
        DATABASE_INCREASE::recordHead* head = (DATABASE_INCREASE::recordHead*) data;
        if (m_offset.load(std::memory_order_relaxed) + head->size > m_bufSize)
        {
            if(m_offset.load(std::memory_order_relaxed)==0)
            {
                free(m_buf);
                m_buf = (char*)malloc(m_bufSize = head->size);
                if(nullptr == m_buf)
                {
                    LOG(ERROR)<<"alloc "<<m_bufSize<<" byte memory failed";
                    return FAULT;
                }
            }
            else
                return FULL;
        }
        if(m_flag&BLOCK_FLAG_HAS_REDO)
        {
            appendingBlockStaus rtv;
            if(OK!=(rtv = writeRedo(data)))
            {
                LOG(ERROR)<<"write redo log failed";
                return rtv;
            }
        }
        memcpy(m_buf + m_offset.load(std::memory_order_relaxed), data,
                head->size);
        m_offset.fetch_add(head->size, std::memory_order_relaxed);
        m_recordIDs[m_endID-m_startID].id = m_endID;
        m_recordIDs[m_endID-m_startID].offset = m_offset.load(std::memory_order_relaxed);
        if (head->type <= DATABASE_INCREASE::REPLACE)
        {
            DATABASE_INCREASE::DMLRecord::DMLRecordHead * dmlHead =
            (DATABASE_INCREASE::DMLRecord::DMLRecordHead*) (data
                    + head->headSize);
            if(m_flag&BLOCK_FLAG_MULTI_TABLE)
            {
                if(m_tableID != dmlHead->tableMetaID)
                {

                }

            }
            else
            {

            }
            tableIndexMap::iterator iter= m_tables.find(dmlHead->tableMetaID);
            if (iter!=m_tables.end())
            {
                void * data = iter.key().data;
                m_appendIndexFuncs[0](data,)

            }
            else
            {
                tableKV
                m_tables.Insert()
                return FAULT;
            }

        }
        else
        {
        }
        m_endID++;
        return OK;
    }
    int flush()
    {
        if (0
                > (m_fd = open(m_path.c_str(), O_RDWR | O_CREAT,
                        S_IRUSR | S_IWUSR | S_IRGRP)))
        {
            LOG(ERROR)<<"open data file :"<<m_path<<" failed for errno:"<<errno<<",error info:"<<strerror(errno);
            return -1;
        }
        if(m_flag&BLOCK_FLAG_COMPRESS)
        {

        }
    }

};

class appendingBlockLineIterator: public iterator
{
private:
    appendingBlock * m_block;
    std::atomic<uint32_t> m_offset;
    filter * m_filter;
    uint32_t m_id;
    appendingBlockLineIterator(appendingBlock * block, filter * filter,
            uint32_t offset = 0) :
            m_block(block), m_filter(filter), m_offset(offset), m_id(0)
    {
        m_block->m_ref.fetch_add(1);
        assert(
                m_offset.load(std::memory_order_relaxed)
                        <= m_block->m_offset.load(std::memory_order_release));
    }
    ~appendingBlockLineIterator()
    {
        m_block->m_ref.fetch_sub(1);
    }
    inline const char * value()
    {
        return m_block->m_buf + m_offset.load(std::memory_order_relaxed);
    }
    inline status next()
    {
        do
        {
            if (m_offset.load(std::memory_order_relaxed)
                    == m_block->m_offset.load(std::memory_order_release))
            {
                if (m_block->m_status != OK)
                    return m_block->m_status;
                m_block->m_cond->m_waiting.push(m_id);
            }
            assert(
                    m_offset.load(std::memory_order_relaxed)
                            < m_block->m_offset.load(
                                    std::memory_order_relaxed));
            m_offset += ((DATABASE_INCREASE::recordHead*) (m_block->m_buf
                    + m_offset.load(std::memory_order_relaxed)))->size;
        } while (m_filter->doFilter(value()));
        return true;
    }
};
class appendingBlockIterator: public iterator
{
private:
    appendingBlock * m_block;
    std::atomic<uint32_t> m_offset;
    filter * m_filter;
    uint32_t m_id;
    appendingBlockIterator(appendingBlock * block, filter * filter,
            uint32_t offset = 0) :
            m_block(block), m_filter(filter), m_offset(offset), m_id(0)
    {
        m_block->m_ref.fetch_add(1);
        assert(
                m_offset.load(std::memory_order_relaxed)
                        <= m_block->m_offset.load(std::memory_order_release));
    }
    ~appendingBlockIterator()
    {
        m_block->m_ref.fetch_sub(1);
    }
    inline void* value() const
    {
        return m_block->m_buf + m_offset.load(std::memory_order_relaxed);
    }
    inline status next()
    {
        do
        {
            if (m_offset.load(std::memory_order_relaxed)
                    == m_block->m_offset.load(std::memory_order_release))
            {
                if (m_block->m_status != OK)
                    return m_block->m_status;
                m_block->m_cond->m_waiting.push(m_id);
            }
            assert(
                    m_offset.load(std::memory_order_relaxed)
                            < m_block->m_offset.load(
                                    std::memory_order_relaxed));
            m_offset += ((DATABASE_INCREASE::recordHead*) (m_block->m_buf
                    + m_offset.load(std::memory_order_relaxed)))->size;
        } while (m_filter->doFilter(value()));
        return true;
    }
    inline bool end()
    {

    }
    inline void wait()
    {

    }
    inline bool valid()
    {

    }
};
}

