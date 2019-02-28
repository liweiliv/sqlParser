/*
 * nonBlockLinkList.h
 *
 *  Created on: 2018年12月20日
 *      Author: liwei
 */

#ifndef NONBLOCKLINKLIST_H_
#define NONBLOCKLINKLIST_H_
#include <atomic>
struct nonBlockLinkListNode
{
    std::atomic<nonBlockLinkListNode*> prev;
    std::atomic<nonBlockLinkListNode*> next;
    nonBlockLinkListNode():prev(nullptr),next(nullptr){}
};
struct nonBlockLinkList
{
    nonBlockLinkListNode head;
    void append(nonBlockLinkListNode &node)
    {
        nonBlockLinkListNode * prev = head.prev;

    }
};
#ifndef container_of
#define container_of(ptr, type, member) ({ \
     const decltype( ((type *)0)->member ) *__mptr = (ptr); \
     (type *)( (char *)__mptr - offsetof(type,member) );})
#endif
#define BNL_GET_VALUE(pnode,type,member) container_of(pnode,type,member)


#endif /* NONBLOCKLINKLIST_H_ */
