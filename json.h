/*
 * json.h
 *
 *  Created on: 2018年11月14日
 *      Author: liwei
 */

#ifndef SRC_CONGO_DRC_LIB_MYSQLPARSER_JSON_H_
#define SRC_CONGO_DRC_LIB_MYSQLPARSER_JSON_H_
#ifndef JSON_H_
#define JSON_H_
#include <list>
#include <string>
#include <stdint.h>
using namespace std;
class jsonValue
{
public:
    enum type
    {
        STRING,
        NUM,
        OBJECT,
        ARRAY,
        BOOL,
        NULLTYPE
    };
    type t;
    jsonValue(type _t);
    static type getType(const char * data);
    static jsonValue * Parse(const char* data,int &size);
};
class jsonString :public jsonValue
{
public:
    std::string m_value;
    jsonString(const char * data=NULL);
    int parse(const char * data);
};
class jsonNum :public jsonValue
{
public:
    long m_value;
    jsonNum(const char * data = NULL);
    int parse(const char * data);
};
class jsonObject :public jsonValue
{
public:
    struct ObjectKV
    {
        jsonValue * key;
        jsonValue * value;
    };
    std::list<ObjectKV*> m_values;
    jsonObject(const char * data=NULL);
    jsonValue * get(string s);
    ~jsonObject();
    void clean();
    int parse(const char * data);
};
class jsonArray :public jsonValue
{
public:
    std::list<jsonValue*> m_values;
    jsonArray(const char * data=NULL);
    ~jsonArray();
    void clean();
    int parse(const char * data);
};
class jsonBool :public jsonValue
{
public:
    bool m_value;
    jsonBool(const char * data = NULL);
    int parse(const char * data);

};
#endif /* SRC_CONGO_DRC_LIB_MYSQLPARSER_JSON_H_ */
