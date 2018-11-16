/*
 * stackLog.cpp
 *
 *  Created on: 2017年9月20日
 *      Author: liwei
 */

#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <pthread.h>
#include <stdarg.h>
#include <execinfo.h>
#include "stackLog.h"
#include <string.h>
using namespace std;
struct stackLog
{
    pthread_t tid;
    void * stacks[512];
    char ** logs;
    int * codes;
    int stacksSize;
};
static std::map<pthread_t,stackLog*> *stackLogMap = NULL;
static pthread_mutex_t * stackLogMapLock = NULL;
static void __cleanStackLog(stackLog * sl)
{
    if(sl == NULL)
        return;
    if(sl->codes)
    {
        free(sl->codes);
        sl->codes = NULL;
    }
    memset(sl->stacks,0,sizeof(sl->stacks));
    if(sl->logs)
    {
        for(int i=0;i<sl->stacksSize;i++)
        {
            if(sl->logs[i]!=NULL)
                free(sl->logs[i]);
        }
        free(sl->logs);
        sl->logs = NULL;
    }
    sl->stacksSize = 0;
}
int initStackLog()
{
    if(stackLogMap!=NULL||stackLogMapLock!=NULL)
        return -1;
    stackLogMap = new map<pthread_t,stackLog*>;
    stackLogMapLock = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(stackLogMapLock,NULL);
    return 0;
}

int destroyStackLog()
{
    if(stackLogMap==NULL||stackLogMapLock==NULL)
        return -1;
    for(std::map<pthread_t,stackLog*>::iterator iter = stackLogMap->begin();iter!=stackLogMap->end();iter++)
    {
        stackLog * sl = iter->second;
        __cleanStackLog(sl);
        free(sl);
    }
    delete stackLogMap;
    stackLogMap = NULL;
    pthread_mutex_destroy(stackLogMapLock);
    free(stackLogMapLock);
    stackLogMapLock = NULL;
    return 0;
}
/*
 * 获取当前线程的stackLog，默认不存在返回NULL，如果create =true ，不存在则为当前线程创建一个
 * */
static stackLog * getCurrentThreadStackLog(bool create = false)
{
    stackLog * sl = NULL;
    pthread_t tid = pthread_self();
    pthread_mutex_lock(stackLogMapLock);
    std::map<pthread_t,stackLog*>::iterator iter = stackLogMap->find(tid);
    if(iter==stackLogMap->end())
    {
        pthread_mutex_unlock(stackLogMapLock);
        if(create)
        {
            sl = (stackLog*)malloc(sizeof(stackLog));
            if(sl==NULL)
                abort();
            sl->tid = tid;
            sl->logs = NULL;
            sl->codes = NULL;
            sl->stacksSize =0;
            memset(sl->stacks,0,sizeof(sl->stacks));
            pthread_mutex_lock(stackLogMapLock);
            stackLogMap->insert(pair<pthread_t,stackLog*>(tid,sl));
            pthread_mutex_unlock(stackLogMapLock);
            return sl;
        }
        else
            return NULL;
    }
    else
    {
        sl = iter->second;
        pthread_mutex_unlock(stackLogMapLock);
        return sl;
    }
}
/*清理当前线程的stacklog
 * */
void cleanStackLog()
{
    if(stackLogMap==NULL||stackLogMapLock==NULL)
        abort();
    __cleanStackLog(getCurrentThreadStackLog());
}
/*
 * 写log，一般使用SET_STACE_LOG_AND_RETURN或者SET_STACE_LOG来调用
 */
void setStackLog(int codeLine,const char * func,const char * file,int code,const char * fmt,...)
{
    stackLog * sl = getCurrentThreadStackLog(true);
    void * stack[256] = {0};
    /*获取函数栈*/
    int stack_num = backtrace(stack, 256);
    /**/
    if(stack_num>=sl->stacksSize+1||memcmp(&sl->stacks[sl->stacksSize-stack_num+2],&stack[2],sizeof(void*)*(stack_num-2))!=0)
    {
        __cleanStackLog(sl);
        sl->stacksSize = stack_num -1;
        sl->codes = (int*)malloc(sl->stacksSize*sizeof(int));
        memset(sl->codes,0,sl->stacksSize*sizeof(int));
        sl->logs = (char**)malloc(sl->stacksSize*sizeof(char*));
        memset(sl->logs,0,sl->stacksSize*sizeof(char*));
        memcpy(sl->stacks,&stack[1],sizeof(void*)*(stack_num-1));
    }
    char * msg = NULL;
    int msgLen = 0;
    if (fmt != NULL && fmt[0] != '\0')
    {
        va_list ap;
        va_start(ap, fmt);
        msgLen = vasprintf(&msg, fmt, ap);
        va_end(ap);
        if (msgLen == -1)
            abort();
    }
    char * fullMsg ;
    msgLen = asprintf(&fullMsg, "@%s:%d.%s() [%s]",file==NULL?"unknown source file":file,codeLine,func,msg);
    if (msgLen == -1)
        abort();
    free(msg);
    sl->codes[stack_num -2] = code;
    sl->logs[stack_num -2] = fullMsg;
}

int getChildLogDetail(int &code,const char *&log)
{
    stackLog *sl = getCurrentThreadStackLog();
    if(sl==NULL)
        return -1;
    void * stack[256] = {0};
    int stack_num = backtrace(stack, 256);
    if(stack_num-1>=sl->stacksSize)
        return -1;
    else
    {
        code = sl->codes[stack_num-1];
        log = sl->logs[stack_num-1];
        return 0;
    }
}
int getChildLog(std::string &errorLog)
{
	const char * log;
	int code;
	if(getChildLogDetail(code,log)!=0||log==NULL)
		return -1;
	errorLog = log;
	return 0;
}
void  getFullStackLog(string &log)
{
    stackLog *sl = getCurrentThreadStackLog();
    if(sl==NULL||sl->stacksSize==0)
        return ;
    char ** stacktrace = backtrace_symbols(sl->stacks, sl->stacksSize);
    for(int i=sl->stacksSize-1;i>=0;i--)
    {
        if(sl->logs[i]==NULL)
        {
            log += "@";
            log += stacktrace[sl->stacksSize-i-1];
        }
        else
            log += sl->logs[i];
        if(i!=0)
            log +='\n';
        else
            break;
    }
    return ;
}



