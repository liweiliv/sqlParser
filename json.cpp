/*
 * json.cpp
 *
 *  Created on: 2018年10月30日
 *      Author: liwei
 */
#include <string.h>
#include <stdio.h>
#include "json.h"
jsonValue::type jsonValue::getType(const char * data)
{
    if (data == NULL)
        return NULLTYPE;
    const char * ptr = data;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (*ptr == '{')
        return OBJECT;
    else if (*ptr == '[')
        return ARRAY;
    else if (*ptr == '"')
        return STRING;
    else if (*ptr <= '9' && *ptr >= '0')
        return NUM;
    else if (*ptr == '-' && ptr[1] <= '9' && ptr[1] >= '0')
        return NUM;
    else if(strncasecmp(ptr,"true",4)==0||strncasecmp(ptr,"false",5)==0)
        return BOOL;
    else
        return NULLTYPE;
}
jsonValue::jsonValue(jsonValue::type _t):t(_t){}

jsonValue * jsonValue::Parse(const char* data, int &size)
{
    switch (getType(data))
    {
    case NUM:
    {
        jsonNum * j = new jsonNum();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;
    }
    case STRING:
    {
        jsonString * j = new jsonString();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;
    }
    case OBJECT:
    {
        jsonObject * j = new jsonObject();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;
    }
    case ARRAY:
    {
        jsonArray * j = new jsonArray();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;
    }
    case BOOL:
    {
        jsonBool * j = new jsonBool();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;

    }
    default:
        return NULL;
    }
}

jsonString::jsonString(const char * data ) :
        jsonValue(STRING)
{
    parse(data);
}
int jsonString::parse(const char * data)
{
    if (data == NULL)
        return 0;
    m_value.clear();
    const char * ptr = data;
    string value;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (ptr[0] != '"')
        return -1;
    const char * e = strchr(ptr + 1, '"');
    if (e == NULL)
        return -2;
    m_value.assign(ptr + 1, e - ptr - 1);
    return e - data + 1;
}
jsonNum::jsonNum(const char * data ) :
        jsonValue(NUM), m_value(0)
{
    parse(data);
}
int jsonNum::parse(const char * data)
{
    m_value = 0;
    bool flag = true;
    if (data == NULL)
        return 0;
    const char * ptr = data;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (*ptr == '-')
    {
        flag = false;
        ptr++;
    }
    if (*ptr > '9' || *ptr < '0')
        return -1;
    m_value = ptr[0] - '0';
    while (*ptr <= '9' && *ptr >= '0')
    {
        m_value *= 10;
        m_value += ptr[0] - '0';
        ptr++;
    }
    if (!flag)
        m_value = -m_value;
    return ptr - data;
}

jsonObject::jsonObject(const char * data ) :
        jsonValue(OBJECT)
{
    parse(data);
}
jsonObject::~jsonObject()
{
    clean();
}
jsonValue * jsonObject::get(string s)
{
    for (list<ObjectKV*>::iterator i = m_values.begin(); i != m_values.end();i++)
     {
            if (*i != NULL)
                {
                if ((*i)->key != NULL&&(*i)->key->t==STRING&&static_cast<jsonString*>((*i)->key)->m_value==s)
                	return (*i)->value;
                }
     }
    return NULL;
}
void jsonObject::clean()
{
    for (list<ObjectKV*>::iterator i = m_values.begin(); i != m_values.end();
            i++)
    {
        if (*i != NULL)
        {
            if ((*i)->key != NULL)
                delete (*i)->key;
            if ((*i)->value != NULL)
                delete (*i)->value;
            delete (*i);
        }
    }
    m_values.clear();
}
int jsonObject::parse(const char * data)
{
    if (data == NULL)
        return 0;
    clean();
    const char * ptr = data;
    string value;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (ptr[0] != '{')
        return -1;
    ptr++;
    while (*ptr != 0)
    {
        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr == '}')
            break;
        int size = 0;
        jsonValue * k = jsonValue::Parse(ptr, size);
        if (k == NULL)
        {
            clean();
            return -1;
        }
        ptr += size;
        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr != ':')
        {
            delete k;
            clean();
            return -1;
        }
        ptr++;
        jsonValue * v = jsonValue::Parse(ptr, size);
        if (v == NULL)
        {
            delete k;
            clean();
            return -1;
        }
        ptr += size;
        ObjectKV * kv = new ObjectKV;
        kv->key = k;
        kv->value = v;
        m_values.push_back(kv);

        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr == '}')
            break;
        else if (*ptr != ',')
        {
            clean();
            return -1;
        }
        ptr++;
    }
    return ptr - data + 1;
}
jsonArray::jsonArray(const char * data) :
        jsonValue(ARRAY)
{
    parse(data);
}
jsonArray::~jsonArray()
{
    clean();
}
void jsonArray::clean()
{
    for (list<jsonValue*>::iterator i = m_values.begin(); i != m_values.end();
            i++)
    {
        if (*i != NULL)
        {
            delete (*i);
        }
    }
    m_values.clear();
}
int jsonArray::parse(const char * data)
{
    if (data == NULL)
        return 0;
    clean();
    const char * ptr = data;
    string value;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (ptr[0] != '[')
        return -1;
    ptr++;
    while (*ptr != 0)
    {
        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr == ']')
            break;
        int size = 0;
        jsonValue * v = jsonValue::Parse(ptr, size);
        if (v == NULL)
        {
            clean();
            return -1;
        }
        m_values.push_back(v);
        ptr+=size;
        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr == ']')
            break;
        else if (*ptr != ',')
        {
            clean();
            return -1;
        }
        ptr++;
    }
    return ptr - data + 1;
}
jsonBool::jsonBool(const char * data):jsonValue(BOOL),m_value(false)
{
    parse(data);
}
int jsonBool::parse(const char * data)
{
    if(data==NULL)
    {
        m_value = false;
        return 0;
    }
    const char * ptr =data;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if(strncasecmp(ptr,"true",4)==0)
    {
        m_value = true;
        return 4+ptr-data;
    }
    else if(strncasecmp(ptr,"false",5)==0)
    {
        m_value = false;
        return 5+ptr-data;
    }
    else
        return -1;
}



