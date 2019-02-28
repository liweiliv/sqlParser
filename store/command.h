/*
 * command.h
 *
 *  Created on: 2019年2月14日
 *      Author: liwei
 */

#ifndef COMMAND_H_
#define COMMAND_H_
#include <stdint.h>

#include "userHandle.h"
#include "userManager.h"
#include "storeError.h"
#include "sqlParser.h"
#include "schedule.h"
namespace STORE
{
#define CMD_AUTH 0x01
#define CMD_SQL  0x02
#define CMD_EXIT 0x03

struct authCmd
{
    /*1 byte username size+username+'\0'+1 byte password md5 +1 byte '\0'*/
    const char * username;
    const char * passWordMd5;
    authCmd(const char *data, uint32_t size) :
            username(nullptr), passWordMd5(nullptr)
    {
        if ((uint8_t) data[0] >= size - 1)
            return;
        username = data + 1;
        if (username[(uint8_t) data[0] - 1] != '\0')
        {
            username = nullptr;
            return;
        }
        passWordMd5 = data + 1 + (uint8_t) data[0] + 1;
        if (*(uint8_t*) (passWordMd5 - 1) < size - 1 - (passWordMd5 - data)
                || passWordMd5[(*(uint8_t*) (passWordMd5 - 1)) - 1] != '\0')
        {
            username = passWordMd5 = nullptr;
            return;
        }
    }
};
struct sqlCmd
{
    const char * sql;
    uint32_t sqlSize;
    sqlCmd(const char *data, uint32_t size) :
            sql(nullptr), sqlSize(0)
    {
        sqlSize = *(uint32_t*) data;
        if (sqlSize < size - sizeof(sqlSize))
        {
            sqlSize = 0;
            return;
        }
        sql = data + sizeof(sqlSize);
        if (sql[sqlSize - 1] != '\0')
        {
            sql = nullptr;
            sqlSize = 0;
            return;
        }
        sqlSize--;
    }
};
class command
{
private:
    userManager * m_userManager;
    sqlParser::sqlParser m_sqlParser;
    schedule m_schedule;
public:
    int processCmd(userHandle * user, const char * cmd, uint32_t size)
    {
        switch ((uint8_t) cmd[0])
        {
        case CMD_AUTH:
        {
            authCmd auth(cmd + 1, size - 1);
            if (auth.passWordMd5 == nullptr || auth.username == nullptr)
            {
                user->setError(ERROR_ILLEGAL_CMD, "unknown command");
                return -1;
            }
            return m_userManager->auth(user, auth.username, auth.passWordMd5)
                    != 0;
        }
        case CMD_SQL:
        {
            if (user->m_user == nullptr)
            {
                user->setError(ERROR_NOT_LOGIN,
                        std::string("do not auth").c_str());
                LOG(WARNING)<<"process sql failed for "<<user->getError();
                return -1;
            }
            if (user->m_user->roles & ROLE_READER)
            {
                user->setError(ERROR_PERMISSION_DENIED,
                        std::string("user: ").append(user->m_user->username).append(
                                " can not query sql").c_str());
                LOG(WARNING)<<"process sql failed for "<<user->getError();
                return -1;
            }
            sqlCmd sql(cmd, size);
            if (sql.sql == nullptr)
            {
                user->setError(ERROR_ILLEGAL_CMD, "unknown command");
                return -1;
            }
            sqlParser::handle *h;
            if (sqlParser::OK != m_sqlParser.parse(h, sql.sql))
            {
                user->setError(ERROR_ILLEGAL_CMD,
                        std::string("parse sql : ").append(sql.sql).append(
                                " failed").c_str());
                LOG(WARNING)<<"process sql failed for "<<user->getError();
                return -1;
            }
            return 0;
    }
    default:
    {
        user->setError(ERROR_ILLEGAL_CMD, "unknown command");
        return -1;
    }

        }
    }
};
}

#endif /* COMMAND_H_ */
