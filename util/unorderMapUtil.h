/*
 * unorderMapUtil.h
 *
 *  Created on: 2019年2月20日
 *      Author: liwei
 */

#ifndef UNORDERMAPUTIL_H_
#define UNORDERMAPUTIL_H_
#include <tr1/unordered_map>
#include <stdint.h>
#include <string.h>
static inline uint32_t _hash(const char* s)
{
    uint32_t hash = 1315423911;
    while (*s)
    {
        hash ^= ((hash << 5) + (*s++) + (hash >> 2));
    }
    return (hash & 0x7FFFFFFF);
}
class StrHash
{
public:
    inline uint32_t operator()(const char * s) const
    {
        return _hash(s);
    }
};
class StrCompare
{
public:
    inline uint32_t operator()(const char * s, const char * d) const
    {
        return strcmp(s, d);
    }
};





#endif /* UNORDERMAPUTIL_H_ */
