/*
 * metaDataCollection.cpp
 *
 *  Created on: 2018年11月5日
 *      Author: liwei
 */
#include <string.h>
#include "metaData.h"
#include "trieTree.h"
#ifndef likely
# define likely(x)  __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
# define unlikely(x)    __builtin_expect(!!(x), 0)
#endif
class tableMetaTimeline
{
private:
    struct checkpoint
    {
        uint64_t fileID;
        uint64_t offset;
    };
    struct tableMetaInfo
    {
        tableMeta * meta;
        checkpoint begin;
        checkpoint end;
        tableMetaInfo * prev;
    };
    tableMetaInfo * m_current;
public:
    tableMetaTimeline(tableMeta * meta = NULL, uint64_t fileID = 0, uint64_t offset = 0):m_current(NULL)
    {
        if(meta!=NULL)
            put(meta,fileID,offset);
    }
    ~tableMetaTimeline()
    {
        purge(0xffffffffffffffffUL,0xffffffffffffffffUL);
    }
    /*can be concurrent*/
    inline tableMetaInfo * get(uint64_t fileID,uint64_t offset)
    {
        checkpoint c = {fileID,offset};
        tableMetaInfo * current = m_current,*first;
        __asm__ __volatile__("lfence" ::: "memory");
        int b = memcmp(&current->begin,&c,sizeof(checkpoint));
        if(likely(b<=0))
        {
            int e = memcmp(&current->end,&c,sizeof(checkpoint));
            if(likely(e>0))
                return current;
            else
            {
                first = current;
                __asm__ __volatile__("lfence" ::: "memory");
                tableMetaInfo * newer = m_current;
                int e = memcmp(&newer->end,&c,sizeof(checkpoint));
                if(e<0)
                    return NULL;
                while(newer!=current)
                {
                    b = memcmp(&newer->begin,&c,sizeof(checkpoint));
                    if(b<0)
                        return newer->meta;
                    else
                        newer = newer->prev;
                }
                abort();
            }
        }
        else
        {
            tableMetaInfo * m = current->prev;
            while(m!=NULL)
            {
                b = memcmp(&m->begin,&c,sizeof(checkpoint));
                if(b<0)
                    return m;
                else
                    m = m->prev;
            }
            return NULL;
        }
    }
    /*must be serial*/
    int  put(tableMeta * meta, uint64_t fileID, uint64_t offset)
    {
        tableMetaInfo * m = new tableMetaInfo;
        m->begin.fileID = fileID;
        m->begin.offset = offset;
        m->end.fileID = 0xffffffffffffffffUL;
        m->end.offset = 0xffffffffffffffffUL;
        if (m_current == NULL)
        {
            __asm__ __volatile__("sfence" ::: "memory");
            m_current = m;
            return 0;
        }
        else
        {
            if(0>=memcmp(&m_current->begin,&m->begin,sizeof(checkpoint)))
            {
                delete m;
                return -1;
            }
            m->prev = m_current;
            m_current->end.fileID = fileID;
            m_current->end.offset = offset;
            __asm__ __volatile__("sfence" ::: "memory");
            m_current = m;
            return 0;
        }
    }
    void purge(uint64_t fileID, uint64_t offset)
    {
        checkpoint c = {fileID,offset};
        tableMetaInfo * m = m_current;
        while(m != NULL)
        {
            if(0<memcmp(&m->end,&c,sizeof(checkpoint)))
                m = m->prev;
            else
                break;
        }
        if(m==NULL)
            return ;
        while(m!=NULL)
        {
            tableMetaInfo * tmp = m->prev;
            if(m->meta!=NULL)
                delete m->meta;
            delete m;
            m = tmp;
        }
    }
    static int destroy(void * m,int force)
    {
        delete static_cast<tableMetaTimeline*>(m);
        return 0;
    }
};
class metaDataCollection
{
private:
    static int destoryDB(void * argv)
    {
        delete static_cast<tireTree*>(argv);
        return 0;
    }
    static int destroyMetas(void* argv)
    {
        delete static_cast<tableMetaTimeline*>(argv);
        return 0;
    }
    tireTree m_dbs;

public:
    metaDataCollection():m_dbs(destoryDB){}
    tableMeta * get(const char * database,const char * table,uint64_t fileID,uint64_t offset)
    {
        tireTree * db = static_cast<tireTree*>(m_dbs.findNCase((const unsigned char*)database));
        if(db == NULL)
            return NULL;
        tableMetaTimeline * metas = static_cast<tableMetaTimeline*>(db->findNCase((const unsigned char*)table));
        if(metas==NULL)
            return NULL;
        return metas->get(fileID,offset);
    }
    int put(const char * database,const char * table,tableMeta * meta,uint64_t fileID,uint64_t offset)
    {
        tireTree * db = static_cast<tireTree*>(m_dbs.findNCase((const unsigned char*)database));
        bool newDB = false,newMeta = false;
        if(db == NULL)
        {
            newDB = true;
            db = new tireTree(destroyMetas);
        }
        tableMetaTimeline * metas = static_cast<tableMetaTimeline*>(db->findNCase((const unsigned char*)table));
        if(metas == NULL)
        {
            newMeta = true;
            metas = new tableMetaTimeline(meta,fileID,offset);
        }
        else
        {
            metas->put(meta,fileID,offset);
        }
        if(newMeta)
        {
            __asm__ __volatile__("mfence" ::: "memory");
            db->insertNCase((const unsigned char*)table,metas);
        }
        if(newDB)
        {
            __asm__ __volatile__("mfence" ::: "memory");
            m_dbs.insertNCase((const unsigned char*)database,db);
        }
        return 0;
    }
};



