/*
 * sqlParser.h
 *
 *  Created on: 2018年11月21日
 *      Author: liwei
 */

#ifndef SQLPARSER_H_
#define SQLPARSER_H_
#include <string>
#include <map>
#include "metaChangeInfo.h"
class jsonValue;
namespace sqlParser
{
class SQLWord;
class sqlParser
{
private:
    std::map<uint32_t, SQLWord *> m_parseTree;
    std::map<uint32_t, SQLWord *> m_parseTreeHead;
    void * m_funcsHandel;
    SQLWord* loadSQlWordFromJson(jsonValue *json);
public:
    sqlParser();
    ~sqlParser();
    int LoadFuncs(const char * fileName);
    int LoadParseTree(const char *config);
    int LoadParseTreeFromFile(const char * file);
public:
    parseValue parse(handle *&h, const char * sql);
};
};
#endif /* SQLPARSER_H_ */
