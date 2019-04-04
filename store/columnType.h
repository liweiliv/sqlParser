#pragma once
#define UNION 0
#define UINT8 1
#define INT8  2
#define UINT16 3
#define INT16 4
#define UINT32 5
#define INT32 6
#define UINT64 7
#define INT64 8
#define BIG_NUMBER 9
#define FLOAT 10
#define DOUBLE 11
#define DECIMAL 12
#define TIMESTAMP 13
#define DATETIME 14
#define DATE 15
#define YEAR 16
#define TIME 17
#define BLOB 18
#define STRING 19
#define JSON 20
#define XML 21
#define GEOMETRY 22
#define SET 23
#define ENUM 24
#define MAX_TYPE 255
struct columnTypeInfo
{
	uint8_t type;
	uint8_t columnTypeSize;
	bool asIndex;
	bool fixed;
};
constexpr static columnTypeInfo columnInfos[] = {
{UNION,4,true,false,},
{UINT8, 1,true,true},
{INT8,1 ,true,true},
{UINT16,2,true,true},
{INT16, 2,true,true},
{UINT32,4,true,true},
{INT32,4 ,true,true},
{UINT64,8,true,true}, 
{INT64,8,true,true},
{BIG_NUMBER,4 ,false,false},
{FLOAT,4,true,true},
{DOUBLE,8,true,true},
{DECIMAL,4,true,true},
{TIMESTAMP,8 ,true,true},
{ DATETIME,8,false,true},
{DATE,2,false,true},
{YEAR,1,false,true},
{TIME,4,false,true},
{BLOB,4,false,true},
{STRING,4,false,true},
{JSON,4,false,true},
{XML,4,false,true},
{GEOMETRY,4,false,true},
{SET,8,false,true},
{ENUM,2,false,true}
};
