/*
 * iterator.h
 *
 *  Created on: 2019年1月15日
 *      Author: liwei
 */
namespace STORE
{
class userData;
class iterator
{
public:
    enum status{
        OK,
        BLOCKED,
        INVALID,
        ENDED
    };
    iterator *m_next;
    iterator *m_prev;
    userData * m_user;
public:
    virtual bool valid() = 0;
    virtual status next() = 0;
    virtual void* value() const = 0;
    virtual void wait() = 0;
    virtual bool end() = 0;
};
}



