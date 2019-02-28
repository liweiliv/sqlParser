/*
 * config.h
 *
 *  Created on: 2019年1月18日
 *      Author: liwei
 */

#ifndef CONFIG_H_
#define CONFIG_H_
#include <map>
#include <string>
#include <pthread.h>
#include <stdio.h>
#include <errno.h>
#include <error.h>
#include <glog/logging.h>
class config{
private:
    std::map<std::string, std::map<std::string,std::string>* > m_sections;
    std::string m_filePath;
    pthread_rwlock_t m_lock;
public:
    std::string get(const char* section,const char* key,const char * defaultValue = NULL)
    {
        pthread_rwlock_rdlock(&m_lock);
        std::map<std::string, std::map<std::string,std::string>* >::const_iterator secIter = m_sections.find(section);
        if(secIter == m_sections.end())
        {        pthread_rwlock_unlock(&m_lock);
            return defaultValue?defaultValue:std::string();
        }
        std::map<std::string,std::string>::const_iterator kvIter = secIter->second->find(key);
        if(kvIter == secIter->second->end())
        {
            pthread_rwlock_unlock(&m_lock);
            return defaultValue?defaultValue:std::string();
        }
        else
        {
            std::string value = kvIter->second;
            pthread_rwlock_unlock(&m_lock);
            return value;
        }
    }
    void set(const char* section,const char* key,const char *value)
    {
        pthread_rwlock_wrlock(&m_lock);
        std::map<std::string, std::map<std::string,std::string>* >::iterator secIter = m_sections.find(section);
        if(secIter == m_sections.end())
            secIter = m_sections.insert(
                    std::pair<std::string, std::map<std::string,std::string>* >(section,new std::map<std::string,std::string>())).first;
        std::map<std::string,std::string>::iterator kvIter = secIter->second->find(key);
        if(kvIter == secIter->second->end())
            secIter->second->insert(std::pair<std::string,std::string>(key,value));
        else
            kvIter->second = value;
        pthread_rwlock_unlock(&m_lock);
    }
    static std::string trim(const char * str,size_t size)
    {
        char *start = str,*end = str+size-1;
        while(start-str<size&&(*start=='\t'||*start==' '||*start=='\n'))
            start++;
        while(end>=start&&(*end=='\t'||*end==' '||*end=='\n'))
            end--;
        if(end<start)
            return std::string();
        else
            return std::string(start,end-start);
    }
    int load()
    {
        if(m_filePath.empty())
        {
            LOG(ERROR)<<"parameter filePath is empty ,load config failed";
            return -1;
        }
        FILE * fp = fopen(m_filePath.c_str(),"r");
        if(fp == NULL)
        {
            LOG(ERROR)<<"open filePath  "<<m_filePath<<" failed for: "<<errno<<" , "<<strerror(errno);
            return -1;
        }
        char buf[1024] = {0};
        char * equal,*hashtag;
        std::string section;
        while(!feof(fp))
        {
            if(NULL == fgets(buf,1023,fp))
                break;
            /*trim and filter data after [#]*/
            while((hashtag = strchr(buf,'#'))!=NULL)
            {
                if(hashtag!=&buf[0]&&*(hashtag-1)=='\\'&&(hashtag<&buf[2]||*(hashtag-2)!='\\'))//[\#] is Escape
                    continue;
                *hashtag = '\0';
                break;
            }
            int16_t size = strlen(buf);
            std::string line = trim(buf,size);

            if(NULL==(equal = strstr(buf,"=")))//section
            {
                section = line;
                continue;
            }
            else //key and value
            {
                std::string key = trim(line.c_str(),equal-line.c_str()-1);
                std::string value = trim(equal+1,line.size()-(equal-line.c_str()+1));
                set(section.c_str(),key.c_str(),value.c_str());
            }
        }
        fclose(fp);
        return 0;
    }
};




#endif /* CONFIG_H_ */
