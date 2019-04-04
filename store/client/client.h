/*
 * client.h
 *
 *  Created on: 2019年1月8日
 *      Author: liwei
 */

#ifndef CLIENT_H_
#define CLIENT_H_
#include <string>
namespace STORE {
	class client {
	public:
		enum CLIENT_STATUS {
			CONNECTING,
			DISCONNECTING,
			DISCONNECTED,
			IDLE,
			STREAM
		};
	private:
		std::string m_host;
		uint16_t m_port;
		std::string m_user;
		std::string m_pass;
		CLIENT_STATUS m_status;
		int m_socket;
	public:
		client(const char * host,uint16_t port,const char* user,const char * pass):m_host(host?host:""),m_port(port),m_user(user?user:""),m_pass(pass?pass:""),m_status(DISCONNECTED), m_socket(-1){

		}
		CLIENT_STATUS getStatus() {
			return m_status;
		}
		int connect()
		{
			if (m_status != DISCONNECTED)
				return -1;
			m_status = CONNECTING;
			//todo socket
			m_status = IDLE;
			return 0;
		}
		const char * askMeta(uint64_t tableID)
		{
			if (m_status != IDLE)
				return nullptr;
			//todo
		}
		int doQuery(const char * query, uint32_t querySize)
		{
			if (m_status != IDLE)
				return -1;
			//todo
			m_status = STREAM;
		}
		const char * stream()
		{

		}
	};
}
#endif
