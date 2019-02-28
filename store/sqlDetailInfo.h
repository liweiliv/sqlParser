/*
 * sqlDetailInfo.h
 *
 *  Created on: 2019年2月20日
 *      Author: liwei
 */

#ifndef SQLDETAILINFO_H_
#define SQLDETAILINFO_H_
#include <string>
#include <stdint.h>
namespace STORE{
enum sqlType{
    INSERT,
    UPDATE,
    DELETE,
    REPLACE,
    SELECT,
    CREATE_DATABASE,
    DROP_DATABASE,
    CREATE_TABLE,
    CREATE_TABLE_LIKE,
    DROP_TABLE,
    ALTER_TABLE_ADD_COLUMN,
    ALTER_TABLE_DROP_COLUMN,
    ALTER_TABLE_ADD_PRIMARY_KEY,
    ALTER_TABLE_ADD_KEY,
    ALTER_TABLE_DROP_KEY,
    CREATE_STREAM,
    DROP_STREAM,
    CREATE_WINDOW_FUNC,
    DROP_WINDOW_FUNC
};
struct sqlDetailInfo{
    sqlType type;
};
struct sqlInsert :public sqlDetailInfo{
    const char * db;
    const char * table;
    const char **columnNames;
    uint32_t rowCount;
    const char **values;
    uint32_t * valueSizes;
};
struct sqlUpdate :public sqlDetailInfo{
    const char * db;
    const char * table;
    const char **columnNames;
    const char **values;
    uint32_t * valueSizes;
    const char ** whereFileds;
    const char ** whereFiledValues;
    uint32_t * whereValueSizes;
};
struct sqlDelete :public sqlDetailInfo{
    const char * db;
    const char * table;
    const char ** whereFileds;
    const char ** whereFiledValues;
    uint32_t * whereValueSizes;
};
struct sqlStream{
    const char * database;
    const char ** tableOrFuncs;
    uint64_t startTimestamp;
    uint64_t startRecordID;
    uint64_t startOriginCheckpoint;
    uint32_t windowSizeByMicroSecond;
    uint32_t windowSizeByRowNum;
};
struct sqlSelect :public sqlDetailInfo{
    const char * db;
    const char * table;
    const char **filedNames;
    const char ** whereFileds;
    const char ** whereFiledValues;
    uint32_t * whereValueSizes;
};
}



#endif /* SQLDETAILINFO_H_ */
