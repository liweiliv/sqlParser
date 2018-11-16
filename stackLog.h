/*
 * stackLog.h
 *
 *  Created on: 2017年9月20日
 *      Author: liwei
 */

#ifndef SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_STACKLOG_H_
#define SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_STACKLOG_H_
#include <string>
#include <string.h>
int initStackLog();
int destroyStackLog();
void cleanStackLog();
#define SET_STACE_LOG(code,...) setStackLog(__LINE__,__func__,basename(__FILE__),code,__VA_ARGS__)
#define SET_STACE_LOG_AND_RETURN(rtv,code,...) setStackLog(__LINE__,__func__,basename(__FILE__),code,__VA_ARGS__);return (rtv);
#define SET_STACE_LOG_(code,...) Log_r::Error(__VA_ARGS__);setStackLog(__LINE__,__func__,basename(__FILE__),code,__VA_ARGS__);
#define SET_STACE_LOG_AND_RETURN_(rtv,code,...) Log_r::Error(__VA_ARGS__); setStackLog(__LINE__,__func__,basename(__FILE__),code,__VA_ARGS__);return (rtv);

void setStackLog(int codeLine,const char * func,const char * file,int code,const char * fmt,...);
int getChildLogDetail(int &code,const char *&log);
int getChildLog(std::string &errorLog);
void  getFullStackLog(std::string &log);
#endif /* SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_STACKLOG_H_ */
