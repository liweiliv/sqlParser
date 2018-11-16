/*
 * tireTree.cpp
 *
 *  Created on: 2018年11月5日
 *      Author: liwei
 */
#include "tireTree.h"
#include <string.h>
#include <assert.h>
#define TT_VALUE_MASK 0x8000000000000000UL
#define TT_NODE_LOW_MASK 0x07
#define TT_NODE_LOW_LEN 3
#define GET_NODE_LOW_OFFSET(v) ((v)&TT_NODE_LOW_MASK)

#define TT_NODE_MID_MASK 0x38
#define TT_NODE_MID_LEN 3
#define GET_NODE_MID_OFFSET(v) (((v)&TT_NODE_MID_MASK)>>(TT_NODE_LOW_LEN))

#define TT_NODE_HIGH_MASK 0xC0
#define TT_NODE_HIGH_LEN 2
#define GET_NODE_HIGH_OFFSET(v) (((v)&TT_NODE_HIGH_MASK)>>(TT_NODE_LOW_LEN+TT_NODE_MID_LEN))

void * tireTree::node::get(uint8_t c)
{
    uint8_t off = GET_NODE_HIGH_OFFSET(c);
    void ** next;
    if ((next = static_cast<void**>(child[off])) != NULL)
    {
        off = GET_NODE_MID_OFFSET(c);
        if ((next = static_cast<void**>(next[off])) != NULL)
            return next[GET_NODE_LOW_OFFSET(c)];
    }
    return NULL;
}
bool tireTree::node::put(uint8_t c, void *value)
{
    void ** mid, **low;
    bool newMid = false;
    bool newLow = false;
    if (child == NULL)
    {
        void ** tmp = (void**) malloc(sizeof(void*) * (1 << TT_NODE_HIGH_LEN));
        memset(tmp, 0, sizeof(void*) * (1 << TT_NODE_HIGH_LEN));
        __asm__ __volatile__("mfence" ::: "memory");
        child = tmp;
    }
    uint8_t _h = GET_NODE_HIGH_OFFSET(c);
    if ((mid = static_cast<void**>(child[_h])) == NULL)
    {
        mid = (void**) malloc(sizeof(void*) * (1 << TT_NODE_MID_LEN));
        memset(mid, 0, sizeof(void*) * (1 << TT_NODE_MID_LEN));
        newMid = true;
    }
    uint8_t _m = GET_NODE_MID_OFFSET(c);
    if ((low = static_cast<void**>(mid[_m])) == NULL)
    {
        low = (void**) malloc(sizeof(void*) * (1 << TT_NODE_LOW_LEN));
        memset(low, 0, sizeof(void*) * (1 << TT_NODE_LOW_LEN));
        newLow = true;

    }
    uint8_t _l = GET_NODE_LOW_OFFSET(c);
    if (low[_l] == NULL)
    {
        low[_l] = value;
        if (newLow)
        {
            __asm__ __volatile__("mfence" ::: "memory");
            mid[_m] = low;
        }
        if (newMid)
        {
            __asm__ __volatile__("mfence" ::: "memory");
            child[_h] = mid;
        }
        return true;
    }
    else
    {
        if (((uint64_t)low[_l] & TT_VALUE_MASK) && !((uint64_t)value & TT_VALUE_MASK))
        {
            static_cast<node*>(value)->put(0, low[_l]);
            low[_l] = value;
            if (newLow)
            {
                __asm__ __volatile__("mfence" ::: "memory");
                mid[_m] = low;
            }
            if (newMid)
            {
                __asm__ __volatile__("mfence" ::: "memory");
                child[_h] = mid;
            }
            return true;
        }
        else
            return false;

    }
}
tireTree::node::node(uint8_t _c) :
        c(_c), num(0), child(NULL)
{
    child = (void**)malloc(sizeof(void*)*(1 << TT_NODE_HIGH_LEN));
    memset(child,0,sizeof(void*)*(1 << TT_NODE_HIGH_LEN));
}
tireTree::node::~node()
{
    void ** mid, **low;
    if (child != NULL)
    {
        for (uint8_t h = 0; h < (1 << TT_NODE_HIGH_LEN); h++)
        {
            if ((mid = static_cast<void**>(child[h])) != NULL)
            {
                for (uint8_t m = 0; m < (1 << TT_NODE_MID_LEN); m++)
                {
                    if ((low = static_cast<void**>(mid[m])) != NULL)
                        free(low);
                }
                free(mid);
            }
        }
        free(child);
        child = NULL;
    }
}

tireTree::node::iterator::iterator()
{
    clear();
}
tireTree::node::iterator::iterator(const tireTree::node::iterator &iter) :
        m_node(iter.m_node), h(iter.h), m_mid(iter.m_mid), m(iter.m), m_low(
                iter.m_low), l(iter.l)
{
}
tireTree::node::iterator &tireTree::node::iterator::operator =(
        const tireTree::node::iterator &iter)
{
    m_node = iter.m_node;
    h = iter.h;
    m_mid = iter.m_mid;
    m = iter.m;
    m_low = iter.m_low;
    l = iter.l;
    return *this;
}
bool tireTree::node::iterator::valid()
{
    return m_node && m_node->child && m_mid && m_low;
}
void tireTree::node::iterator::clear()
{
    m_node = NULL;
    h = 0;
    m_mid = NULL;
    m = 0;
    m_low = NULL;
    l = 0;
}
void * tireTree::node::iterator::value()
{
    if (valid())
        return m_low[l];
    else
        return NULL;
}
const tireTree::node * tireTree::node::iterator::getNode()
{
    return m_node;
}
uint8_t tireTree::node::iterator::key()
{
    return h * (1 << (TT_NODE_MID_LEN + TT_NODE_LOW_LEN))
            + m * (1 << TT_NODE_LOW_LEN) + l;
}
bool tireTree::node::iterator::next()
{
    if (!valid())
        return false;
    l++;
    for (; h < (1 << TT_NODE_HIGH_LEN); h++)
    {
        if ((m_mid = static_cast<void**>(m_node->child[h])) != NULL)
        {
            for (; m < (1 << TT_NODE_MID_LEN); m++)
            {
                if ((m_low = static_cast<void**>(m_mid[m])) != NULL)
                {
                    for (; l < (1 << TT_NODE_LOW_LEN); l++)
                    {
                        if (m_low[l] != NULL)
                            return true;
                    }
                    l = 0;
                }
            }
            l = m = 0;
        }
    }
    return false;
}
tireTree::node::iterator tireTree::node::begin()
{
    tireTree::node::iterator iter;
    iter.m_node = this;
    for (; iter.h < (1 << TT_NODE_HIGH_LEN); iter.h++)
    {
        if ((iter.m_mid = static_cast<void**>(child[iter.h])) != NULL)
        {
            for (; iter.m < (1 << TT_NODE_MID_LEN); iter.m++)
            {
                if ((iter.m_low = static_cast<void**>(iter.m_mid[iter.m]))
                        != NULL)
                {
                    for (; iter.l < (1 << TT_NODE_LOW_LEN); iter.l++)
                    {
                        if (iter.m_low[iter.l] != NULL)
                            return iter;
                    }
                    iter.l = 0;
                }
            }
            iter.l = iter.m = 0;
        }
    }
    iter.clear();
    return iter;
}
tireTree::iterator::iterator()
{
    m_stack.parent = NULL;
    m_top = NULL;
    memset(keyStack, 0, sizeof(keyStack));
    keyStackTop = 0;
}
tireTree::iterator::iterator(const iterator & iter)
{
    m_stack.nodeIter = iter.m_stack.nodeIter;
    m_stack.parent = NULL;
    m_top = NULL;
    memcpy(keyStack, iter.keyStack, sizeof(keyStack));
    keyStackTop = iter.keyStackTop;
    stacks * copy = iter.m_top, *newStack = NULL;
    while (copy != &iter.m_stack)
    {
        if (m_top == NULL)
        {
            m_top = new stacks;
            m_top->nodeIter = iter.m_top->nodeIter;
            newStack = m_top;
        }
        else
        {
            newStack->parent = new stacks;
            newStack->parent->nodeIter = copy->nodeIter;
            newStack = newStack->parent;
        }
        copy = copy->parent;
    }
    if (newStack == NULL)
        m_top = &m_stack;
    else
        newStack->parent = &m_stack;
}
tireTree::iterator &tireTree::iterator::operator =(
        const tireTree::iterator &iter)
{
    m_stack.nodeIter = iter.m_stack.nodeIter;
    m_stack.parent = NULL;
    m_top = NULL;
    stacks * copy = iter.m_top, *newStack = NULL;
    keyStackTop = iter.keyStackTop;
    memcpy(keyStack, iter.keyStack, sizeof(keyStack));
    while (copy != &iter.m_stack)
    {
        if (m_top == NULL)
        {
            m_top = new stacks;
            m_top->nodeIter = iter.m_top->nodeIter;
            newStack = m_top;
        }
        else
        {
            newStack->parent = new stacks;
            newStack->parent->nodeIter = copy->nodeIter;
            newStack = newStack->parent;
        }
        copy = copy->parent;
    }
    if (newStack == NULL)
        m_top = &m_stack;
    else
        newStack->parent = &m_stack;
    return *this;
}
tireTree::iterator::~iterator()
{
    clear();
}
void tireTree::iterator::clear()
{
    if (m_top != NULL)
    {
        while (m_top != &m_stack)
        {
            stacks * p = m_top->parent;
            delete m_top;
            m_top = p;
        }
        m_top = NULL;
    }
    m_stack.nodeIter.clear();
    memset(keyStack, 0, sizeof(keyStack));
    keyStackTop = 0;
}
bool tireTree::iterator::valid()
{
    return (m_top != NULL && m_top->nodeIter.valid()
            && ((uint64_t)m_top->nodeIter.value() & TT_VALUE_MASK));
}
void * tireTree::iterator::value()
{
    if (!valid())
        return NULL;
    return (void*)((uint64_t)m_top->nodeIter.value() & (~TT_VALUE_MASK));
}
const unsigned char *tireTree::iterator::key() //todo
{
    if (!valid())
        return NULL;
    return keyStack;
}
bool tireTree::iterator::next()
{
    bool back = false;
    while (true)
    {
        bool firstOfNode = m_top->nodeIter.key()=='\0';
        if (m_top->nodeIter.next())
        {
            void * v = m_top->nodeIter.value();
            if ((uint64_t)v & TT_VALUE_MASK)
            {
                if(!firstOfNode)
                    keyStack[keyStackTop-1] = m_top->nodeIter.key();
                else
                    keyStack[keyStackTop++] = m_top->nodeIter.key();
                return true;
            }
            else
            {
                if(back)
                {
                    keyStack[--keyStackTop] = '\0';
                    back=false;
                }
                stacks * tmp = new stacks;
                tmp->nodeIter = static_cast<node*>(v)->begin();
                if (!tmp->nodeIter.valid()) //empty node
                {
                    //abort();
                    delete tmp;
                    continue;
                }
                tmp->parent = m_top;
                if(m_top->nodeIter.key()!='\0')
                    keyStack[keyStackTop++] = m_top->nodeIter.key();
                m_top = tmp;
                if ((uint64_t)m_top->nodeIter.value() & TT_VALUE_MASK)
                    return true;
            }
        }
        else
        {
            stacks * parent = m_top->parent;
            if (parent == NULL)
                return false;
            delete m_top;
            m_top = parent;
            keyStack[--keyStackTop] = '\0';
            back = true;
        }
    }
}

tireTree::iterator tireTree::begin()
{
    tireTree::iterator iter;
    iter.m_stack.nodeIter = m_root.begin();
    if (!iter.m_stack.nodeIter.valid())
    {
        iter.clear();
        return iter;
    }
    iter.m_stack.parent = NULL;
    iter.m_top = &iter.m_stack;
    while (!((uint64_t)iter.m_top->nodeIter.value() & TT_VALUE_MASK))
    {
        iterator::stacks * tmp = new iterator::stacks;
        tmp->nodeIter =
                static_cast<node*>(iter.m_top->nodeIter.value())->begin();
        if (!tmp->nodeIter.valid()) //empty node
        {
            //abort();
            delete tmp;
            if (!iter.next())
                iter.clear();
            return iter;
        }
        iter.keyStack[iter.keyStackTop++] = iter.m_top->nodeIter.key();
        tmp->parent = iter.m_top;
        iter.m_top = tmp;
        if ((uint64_t)iter.m_top->nodeIter.value() & TT_VALUE_MASK)
            return iter;
    }
    return iter;
}
tireTree::tireTree(int (*valueDestroyFunc)(void* value)) :
        m_root(0), m_nodeCount(0), m_valueCount(0), m_valueDestroyFunc(
                valueDestroyFunc)
{
}
tireTree::~tireTree()
{
    clear();
}
void tireTree::clear()
{
    iterator iter = begin();
    if (iter.valid())
    {
        void * v = iter.value();
        if (m_valueDestroyFunc)
            m_valueDestroyFunc(v);
        while (true)
        {
            if (iter.m_top->nodeIter.next())
            {
                v = iter.m_top->nodeIter.value();
                if ((uint64_t)v & TT_VALUE_MASK)
                {
                    if (m_valueDestroyFunc)
                        m_valueDestroyFunc((void*)((uint64_t)v & (~TT_VALUE_MASK)));
                    continue;
                }
                iterator::stacks * tmp = new iterator::stacks;
                tmp->nodeIter = static_cast<node*>(v)->begin();
                if (!tmp->nodeIter.valid()) //empty node
                {
                    //abort();
                    delete tmp;
                    continue;
                }
                tmp->parent = iter.m_top;
                iter.m_top = tmp;
                if ((uint64_t)iter.m_top->nodeIter.value() & TT_VALUE_MASK)
                {
                    if (m_valueDestroyFunc)
                        m_valueDestroyFunc(
                                (void*)((uint64_t)iter.m_top->nodeIter.value() &(~TT_VALUE_MASK)));
                    continue;
                }
            }
            else
            {
                iterator::stacks * parent = iter.m_top->parent;
                if (parent == NULL)
                    break;
                delete iter.m_top->nodeIter.getNode();
                delete iter.m_top;
                iter.m_top = parent;
            }
        }
    }
    m_nodeCount = 0;
    m_valueCount = 0;
}
int tireTree::insertNCase(const unsigned char * str,void *value)
{
    node * n = &m_root;
    void * v;
    uint16_t level = 0;
    uint8_t c;
    node * topNewNode = NULL, *topNewNodeParent = NULL;
    uint8_t topNewNodeOff = 0;
    uint16_t newNodes = 0;
SEARCH:
    c = str[level];
    if(c>='A'&&c<='Z')
        c += 'a'-'A';
    if (str[level + 1] != '\0')
    {
        if ((v = n->get(c)) == NULL)
        {
            node * tmp = new node(c);
            newNodes++;
            if (topNewNode == NULL)
            {
                topNewNode = tmp;
                topNewNodeParent = n;
                topNewNodeOff = c;
            }
            else
                n->put(c, tmp);
            n = tmp;
        }
        else
        {
            if ((uint64_t)v & TT_VALUE_MASK)
            {
                node * tmp = new node(c);
                newNodes++;
                if (topNewNode == NULL)
                {
                    topNewNode = tmp;
                    topNewNodeParent = n;
                    topNewNodeOff = c;
                }
                else
                    n->put(c, tmp);
                n = tmp;
            }
            else
                n = static_cast<node*>(v);
        }
        level++;
        goto SEARCH;
    }
    else
    {
        if ((v = n->get(c)) == NULL)
        {
            n->put(c, (void*)((uint64_t)value | TT_VALUE_MASK));
        }
        else
        {
            if ((uint64_t)v & TT_VALUE_MASK)
                return -1;
            static_cast<node*>(v)->put(0, (void*)((uint64_t)value | TT_VALUE_MASK));
        }
        if (topNewNode != NULL)
        {
            __asm__ __volatile__("mfence" ::: "memory");
            topNewNodeParent->put(topNewNodeOff, topNewNode); //add all new node to tree at last
        }
        m_nodeCount += newNodes;
        m_valueCount++;
        return 0;
    }
}
int tireTree::insert(const unsigned char * str, void *value)
{
    node * n = &m_root;
    void * v;
    uint16_t level = 0;
    uint8_t c;
    node * topNewNode = NULL, *topNewNodeParent = NULL;
    uint8_t topNewNodeOff = 0;
    uint16_t newNodes = 0;
    SEARCH: c = str[level];
    if (str[level + 1] != '\0')
    {
        if ((v = n->get(c)) == NULL)
        {
            node * tmp = new node(c);
            newNodes++;
            if (topNewNode == NULL)
            {
                topNewNode = tmp;
                topNewNodeParent = n;
                topNewNodeOff = c;
            }
            else
                n->put(c, tmp);
            n = tmp;
        }
        else
        {
            if ((uint64_t)v & TT_VALUE_MASK)
            {
                node * tmp = new node(c);
                newNodes++;
                if (topNewNode == NULL)
                {
                    topNewNode = tmp;
                    topNewNodeParent = n;
                    topNewNodeOff = c;
                }
                else
                    n->put(c, tmp);
                n = tmp;
            }
            else
                n = static_cast<node*>(v);
        }
        level++;
        goto SEARCH;
    }
    else
    {
        if ((v = n->get(c)) == NULL)
        {
            n->put(c, (void*)((uint64_t)value | TT_VALUE_MASK));
        }
        else
        {
            if ((uint64_t)v & TT_VALUE_MASK)
                return -1;
            static_cast<node*>(v)->put(0, (void*)((uint64_t)value | TT_VALUE_MASK));
        }
        if (topNewNode != NULL)
        {
            __asm__ __volatile__("mfence" ::: "memory");
            topNewNodeParent->put(topNewNodeOff, topNewNode); //add all new node to tree at last
        }
        m_nodeCount += newNodes;
        m_valueCount++;
        return 0;
    }
}
void * tireTree::findNCase(const unsigned char * str)
{
    node * n = &m_root;
    void * v;
    uint16_t level = 0;
    uint8_t c;
SEARCH:
    c = str[level];
    if(c>='A'&&c<='Z')
        c += 'a'-'A';
    if (str[level + 1] != '\0')
    {
        if ((v = n->get(c)) == NULL)
            return NULL;
        if ((uint64_t)v & TT_VALUE_MASK)
            return NULL;
        n = static_cast<node*>(v);
        level++;
        goto SEARCH;
    }
    else
    {
        if ((v = n->get(c)) == NULL)
            return NULL;
        if ((uint64_t)v & TT_VALUE_MASK)
            return (void*)((uint64_t)v & (~TT_VALUE_MASK));
        else
        {
            v = static_cast<node*>(v)->get(0);
            if(v==NULL)
                return NULL;
            if(!((uint64_t)v&TT_VALUE_MASK))
                abort();
            return  (void*)((uint64_t)v&(~TT_VALUE_MASK));
        }
    }
}
void * tireTree::find(const unsigned char * str)
{
    node * n = &m_root;
    void * v;
    uint16_t level = 0;
    uint8_t c;
SEARCH:
    c = str[level];

    if (str[level + 1] != '\0')
    {
        if ((v = n->get(c)) == NULL)
            return NULL;
        if ((uint64_t)v & TT_VALUE_MASK)
            return NULL;
        n = static_cast<node*>(v);
        level++;
        goto SEARCH;
    }
    else
    {
        if ((v = n->get(c)) == NULL)
            return NULL;
        if ((uint64_t)v & TT_VALUE_MASK)
            return (void*)((uint64_t)v & (~TT_VALUE_MASK));
        else
        {
            v = static_cast<node*>(v)->get(0);
            if(v==NULL)
                return NULL;
            if(!((uint64_t)v&TT_VALUE_MASK))
                abort();
            return  (void*)((uint64_t)v&(~TT_VALUE_MASK));
        }
    }
}

