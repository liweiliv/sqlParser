/*
 * userHandel.h
 *
 *  Created on: 2019年2月14日
 *      Author: liwei
 */

#ifndef USERHANDLE_H_
#define USERHANDLE_H_
#include <string>
#include <stdint.h>
namespace STORE{
struct user;
class userHandle{
public:
    std::string m_error;
    uint32_t m_errno;
    user * m_user;
    const char * m_currentCmd;
    uint32_t m_connectId;
    int m_netFD;
public:
    void setError(uint32_t _errno,const char * _error)
    {
        m_error.assign(_error);
        m_errno = _errno;
    }
    const std::string & getError()
    {
        return m_error;
    }
    uint32_t getErrno()
    {
        return m_errno;
    }
};
}




#endif /* USERHANDLE_H_ */
