/*
 * tireTree.h
 *
 *  Created on: 2018年11月6日
 *      Author: liwei
 */

#ifndef SRC_CONGO_DRC_LIB_MYSQLPARSER_TIRETREE_H_
#define SRC_CONGO_DRC_LIB_MYSQLPARSER_TIRETREE_H_
#include <stdint.h>
#include <stdlib.h>
class tireTree
{
private:
    struct node
    {
        uint8_t c;
        uint8_t num;
        void ** child;
        inline void * get(uint8_t c);
        inline bool put(uint8_t c,void *value);
        node(uint8_t _c);
        ~node();
        class iterator
        {
        private:
            friend struct node;
            node * m_node;
            uint8_t h;
            void ** m_mid;
            uint8_t m;
            void ** m_low;
            uint8_t l;
        public:
            iterator();
            iterator(const iterator &iter);
            iterator &operator =(const iterator &iter);
            bool valid();
            void clear();
            void * value();
            const node * getNode();
            uint8_t key();
            bool next();
        };
        iterator begin();
    };
    node  m_root;
    uint32_t m_nodeCount;
    uint32_t m_valueCount;
    int (*m_valueDestroyFunc)(void* value);
public:
    class iterator
    {
    public:
        struct stacks
        {
            node::iterator nodeIter;
            stacks * parent;
        };
    private:
        friend class tireTree;
        stacks m_stack;
        stacks * m_top;
        uint16_t keyStackTop;
        uint8_t keyStack[256];
    public:
        iterator();
        iterator(const iterator & iter);
       iterator &operator =(const iterator &iter);
       ~iterator();
       void clear();
       bool valid();
       void * value();
       const unsigned char *key();
       bool next();
    };
public:
    iterator begin();
    tireTree(int (*valueDestroyFunc)(void* value) = NULL);
    ~tireTree();
    void clear();
     int insert(const unsigned char * str,void *value);
     int insertNCase(const unsigned char * str,void *value);
     void * find(const unsigned char * str);
     void * findNCase(const unsigned char * str);
};


#endif /* SRC_CONGO_DRC_LIB_MYSQLPARSER_TIRETREE_H_ */
