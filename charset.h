#pragma once
/*
 * charset.h
 *
 *  Created on: 2018年12月5日
 *      Author: liwei
 */
#include <stdint.h>
struct charsetInfo {
	const char * name;
	uint8_t byteSizePerChar;
	uint32_t id;
};
#define NO_CHARSET_ID 0xffffu
extern const uint16_t charsetCount;
extern const charsetInfo charsets[];
