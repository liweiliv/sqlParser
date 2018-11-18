/*
 * json.h
 *
 *  Created on: 2018年11月14日
 *      Author: liwei
 */
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
    virtual string toString()=0;
    static type getType(const char * data);
    static jsonValue * Parse(const char* data,int &size);
};
class jsonString :public jsonValue
{
public:
    std::string m_value;
    jsonString(const char * data=NULL);
    int parse(const char * data);
    string toString();
};
class jsonNum :public jsonValue
{
public:
    long m_value;
    jsonNum(const char * data = NULL);
    int parse(const char * data);
    string toString();
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
    string toString();
};
class jsonArray :public jsonValue
{
public:
    std::list<jsonValue*> m_values;
    jsonArray(const char * data=NULL);
    ~jsonArray();
    void clean();
    int parse(const char * data);
    string toString();
};
class jsonBool :public jsonValue
{
public:
    bool m_value;
    jsonBool(const char * data = NULL);
    int parse(const char * data);
    string toString();
};
#endif
