/*
 * userManager.h
 *
 *  Created on: 2019年2月14日
 *      Author: liwei
 */

#ifndef USERMANAGER_H_
#define USERMANAGER_H_
#include <pthread.h>
#include <stdint.h>
#include <map>
#include <unorderMapUtil.h>
#include <string>
#include <string.h>
#include <assert.h>
#include "filter.h"
#include "userHandle.h"
#include "storeError.h"
#include <glog/logging.h>
namespace STORE{
#define ROLE_READER 0x01
#define ROLE_GRANTER 0x02
#define ROLE_STREAM 0x04
struct user{
    std::string username;
    std::string passWordMd5;
    int32_t userID;
    filter allowedTables;
    int32_t roles;
};
class userManager{
private:

    typedef std::tr1::unordered_map<const char *,user*,StrHash,StrCompare> userMap;
    userMap m_users;
    std::map<uint32_t,userHandle*> m_currentUsers;
    pthread_rwlock_t m_lock;
public:
    int loadUser(const char * file)
    {

    }
    int auth(userHandle * handel,const char * user,const char * passWordMd5)
    {
        userMap::iterator iter = m_users.find(user);
        if(iter == m_users.end())
        {
            handel->setError(ERROR_UNKNOWN_USER,std::string("user: ").append(user).append(" not exist").c_str());
            LOG(WARNING)<<"login failed for "<<handel->getError();
            return -1;
        }
        else if(iter->second->passWordMd5.compare(passWordMd5)!=0)
        {
            handel->setError(ERROR_WRONG_PASSWORD,std::string("password of user: ").append(user).append(" not match").c_str());
            LOG(WARNING)<<"login failed for "<<handel->getError();
            return -1;
        }
        else
        {
            LOG(INFO)<<"user "<<user<<" login success";
            handel->m_user =  iter->second;
            uint32_t connectId = 0;
            pthread_rwlock_wrlock(&m_lock);
            std::map<uint32_t,userHandle*>::iterator iter = m_currentUsers.rbegin();
            if(iter!=m_currentUsers.end())
            {
                if(iter->first>=0x7fffffff)
                {
                    iter = m_currentUsers.begin();
                    if((connectId=iter->first)==0)
                    {
                        iter++;
                        for(;iter!=m_currentUsers.end();iter++)
                        {
                            if(iter->first!=connectId+1)
                            {
                                connectId++;
                                break;
                            }
                            else
                                connectId=iter->first;
                        }
                        assert(connectId!=iter->first);
                    }
                }
                else
                    connectId = iter->first+1;
            }
            handel->m_connectId = connectId;
            assert(m_currentUsers.insert(std::pair<uint32_t,userHandle*>(connectId,handel)).second);
            pthread_rwlock_unlock(&m_lock);
            return 0;
        }
    }
    void loginOut(userHandle * handel)
    {
        pthread_rwlock_wrlock(&m_lock);
        m_currentUsers.erase(handel->m_connectId);
        pthread_rwlock_unlock(&m_lock);
    }
    int createNewUser(userHandle * handel,const char * newUserName,const char * newPassWordMd5,const char * filterString,uint32_t roles)
    {
        if(!(handel->m_user->roles&ROLE_GRANTER))
        {
            handel->setError(ERROR_PERMISSION_DENIED,std::string("user: ").append(handel->m_user->username).append(" can not create new user").c_str());
            LOG(WARNING)<<"createNewUser failed for "<<handel->getError();
            return -1;
        }
        if(!handel->m_user->allowedTables.continer(filterString))
        {
            handel->setError(ERROR_PERMISSION_DENIED,std::string("user: ").append(handel->m_user->username).append(" can not access all tables in").append(filterString).c_str());
            LOG(WARNING)<<"createNewUser failed for "<<handel->getError();
            return -1;
        }
        if(handel->m_user->roles|roles!=handel->m_user->roles)
        {
            handel->setError(ERROR_PERMISSION_DENIED,std::string("user: ").append(handel->m_user->username).append(" do not have all roles of new user").c_str());
            LOG(WARNING)<<"createNewUser failed for "<<handel->getError();
            return -1;
        }
        user * newUser = new user;
        newUser->username = newUserName;
        newUser->passWordMd5 = newPassWordMd5;
        newUser->roles = roles;
        if(0!=newUser->allowedTables.init(filterString))
        {
            handel->setError(ERROR_ILLEGAL_CMD,std::string("illegal access table list: ").append(filterString).c_str());
            LOG(WARNING)<<"createNewUser failed for "<<handel->getError();
            delete newUser;
            return -1;
        }
        if(!m_users.insert(std::pair<const char *,user*>(newUser->username.c_str(),newUser)).second)
        {
            handel->setError(ERROR_USER_EXIST,std::string("user ").append(newUserName).append(" exist").c_str());
            LOG(WARNING)<<"createNewUser failed for "<<handel->getError();
            delete newUser;
            return -1;
        }
        LOG(INFO)<<"create new user "<<newUserName<<" , access tables:"<<filterString<<" ,role:"<<roles;
        //todo
        return 0;
    }
};
}




#endif /* USERMANAGER_H_ */
