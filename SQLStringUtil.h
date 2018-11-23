/*
 * strUtil.h
 *
 *  Created on: 2018年11月16日
 *      Author: liwei
 */

#ifndef SQLSTRINGUTIL_H_
#define SQLSTRINGUTIL_H_
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
static inline bool isSpaceOrComment(const char *str)
{
    if ((*str == ' ' || *str == '\t' || *str == '\n'))
        return true;
    if (str[0] == '/' && str[1] == '*')
        return true;
    return false;
}
static inline const char * jumpOverSpace(const char * str)
{
    const char * p = str;
    while (*p == ' ' || *p == '\t' || *p == '\n')
        p++;
    return p;
}
static bool jumpOverComment(const char *& str)
{
    const char * p = jumpOverSpace(str);
    if (strncmp(p, "/*", 2) != 0)
        return false;
    p += 2;
    while (*p != '\0' && (*p != '*' || *(p - 1) == '\\' || *(p + 1) != '/'))
        p++;
    if (*p == '\0')
        return false;
    str = p + 2;
    return true;
}
const char * endOfWord(const char * str);
static const char * realEndOfWord(const char * str)
{
    while (!isSpaceOrComment(str) && *str != '\0')
        str++;
    return str;
}
static const char * nextWord(const char * str)
{
    while (isSpaceOrComment(str))
    {
        jumpOverComment(str);
        str = jumpOverSpace(str);
    }
    return str;
}
static bool getName(const char * str, const char *& start, uint16_t &size,
        const char * &realEnd)
{
    char quote;
    if (*str == '`' || *str == '\'' || *str == '"')
    {
        quote = *str;
        start = str + 1;
        realEnd = strchr(start, quote);
        if (realEnd == NULL)
        {
            return false;
        }
        size = realEnd - start;
        realEnd++;
        return true;
    }
    else
    {
        start = str;
        realEnd = endOfWord(start);
        if (realEnd == start)
            return false;
        size = realEnd - start;
        return true;
    }
}
void initKeyWords();
void destroyKeyWords();
bool isKeyWord(const char * str,uint32_t size);
bool isKeyChar(uint8_t c);
#endif /* SQLSTRINGUTIL_H_ */
