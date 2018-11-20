/*
 * sqlParserFuncs.cpp
 *
 *  Created on: 2018年11月19日
 *      Author: liwei
 */
#include <assert.h>
#include "metaChangeInfo.h"
namespace sqlParser{
#define  NOT_FIXED_DEC 31
#define getLastTable(h) (*(h)->meta.newTables.rbegin())
#define getLastColumn(h) *((getLastTable(h))>newColumns.rbegin())

  extern "C" parseValue bitType(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_BIT;
     c->size = 1;
     return OK;
 }
  extern "C" parseValue bitTypeSize(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->size = atoi(sql.c_str());
     return OK;
 }
  extern "C" parseValue tinyIntType(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_TINY;
     c->m_signed = true;
     return OK;
 }
  extern "C" parseValue BoolType(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_TINY;
     return OK;
 }
  extern "C" parseValue numertypeIsUnsigned(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->m_signed = false;
     return OK;
 }
  extern "C" parseValue smallIntType(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_SHORT;
     return OK;
 }

  extern "C"  parseValue mediumIntType(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_INT24;
     return OK;
 }

  extern "C" parseValue intType(handle * h,const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_LONG;
     return OK;
 }
  extern "C" parseValue bigIntType(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_LONGLONG;
     return OK;
 }
  extern "C" parseValue decimalType(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_NEWDECIMAL;
     c->size = 10;
     c->decimals = 0;
     return OK;
 }
  extern "C" parseValue floatSize(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->size = atoi(sql.c_str());
     return OK;
 }
  extern "C" parseValue floatDigitsSize(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->decimals = atoi(sql.c_str());
     return OK;
 }
  extern "C" parseValue floatType(handle * h, const string& sql)
 {
     newColumnInfo *c = getLastColumn(h);
     c->type = MYSQL_TYPE_FLOAT;
     c->size = 12;
     c->decimals = NOT_FIXED_DEC;
     return OK;
 }
  extern "C" parseValue doubleType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_FLOAT;
      c->size = 22;
      c->decimals = NOT_FIXED_DEC;
      return OK;
  }
  extern "C" parseValue datetimeType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_DATETIME;
      return OK;
  }
  extern "C" parseValue datetimeTypePrec(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_DATETIME2;
      c->precision = atoi(sql.c_str());
      return OK;
  }
  extern "C" parseValue timestampType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_TIMESTAMP;
      return OK;
  }
  extern "C" parseValue timestampTypePrec(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_TIMESTAMP2;
      c->precision = atoi(sql.c_str());
      return OK;
  }
  extern "C" parseValue timeType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_TIME;
      return OK;
  }
  extern "C" parseValue timeTypePrec(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_TIME2;
      c->precision = atoi(sql.c_str());
      return OK;
  }
  extern "C" parseValue yearType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_YEAR;
      return OK;
  }
  extern "C" parseValue yearTypePrec(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->precision = atoi(sql.c_str());
      return OK;
  }
  extern "C" parseValue charType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_STRING;
      c->isString = true;
      return OK;
  }
  extern "C" parseValue varcharType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_VARCHAR;
      c->isString = true;
      return OK;
  }
  extern "C" parseValue stringTypeCharSet(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      assert(c->isString);
      c->charset = sql;
      return OK;
  }
  extern "C" parseValue stringTypeSize(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->size = atol(sql.c_str());
      return OK;
  }
  extern "C" parseValue binaryType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_STRING;
      return OK;
  }
  extern "C" parseValue varbinaryType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_VAR_STRING;
      return OK;
  }
  extern "C" parseValue tinyBlobType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_TINY_BLOB;
      return OK;
  }
  extern "C" parseValue blobType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_BLOB;
      return OK;
  }
  extern "C" parseValue mediumBlobType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_MEDIUM_BLOB;
      return OK;
  }
  extern "C" parseValue longBlobtype(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_LONG_BLOB;
      return OK;
  }
  extern "C" parseValue tinyTextType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_TINY_BLOB;
      c->isString = true;
      return OK;
  }
  extern "C" parseValue textType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_BLOB;
      c->isString = true;
      return OK;
  }
  extern "C" parseValue mediumTextType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_MEDIUM_BLOB;
      c->isString = true;
      return OK;
  }
  extern "C" parseValue longTexttype(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_LONG_BLOB;
      c->isString = true;
      return OK;
  }
  extern "C" parseValue enumType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_ENUM;
      c->isString = true;
      return OK;
  }
  extern "C" parseValue setType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_SET;
      c->isString = true;
      return OK;
  }
  extern "C" parseValue  enumOrSetValueList(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->setAndEnumValueList.push_back(sql);
      return OK;
  }
  extern "C" parseValue geometryType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_GEOMETRY;
      return OK;
  }
  extern "C" parseValue jsonType(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->type = MYSQL_TYPE_JSON;
      c->isString = true;
      c->charset = "utf8mb4";
      return OK;
  }
  extern "C" parseValue newColumn(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      newColumnInfo *c = new newColumnInfo;
      c->name = sql;
      t->newColumns.push_back(c);
      return OK;
  }
  extern "C" parseValue generatedColumn(handle * h, const string& sql)
  {
      newColumnInfo *c = getLastColumn(h);
      c->m_generated = true;
      return OK;
  }
  extern "C" parseValue columnIsUK(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      newColumnInfo *c = getLastColumn(h);
      c->m_isUnique = true;
      newKeyInfo *k = new newKeyInfo();
      k->type = newKeyInfo::UNIQUE_KEY;
      k->name = c->name;
      k->columns.push_back(c->name);
      t->newKeys.push_back(k);
      return OK;
  }
  extern "C" parseValue columnIsPK(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      newColumnInfo *c = getLastColumn(h);
      c->m_isPrimary = true;
      newKeyInfo *k = new newKeyInfo();
      k->type = newKeyInfo::PRIMARY_KEY;
      k->columns.push_back(c->name);
      t->newKeys.push_back(k);
      return OK;
  }
  extern "C" parseValue columnIsKey(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      newColumnInfo *c = getLastColumn(h);
      newKeyInfo *k = new newKeyInfo();
      k->type = newKeyInfo::KEY;
      k->name = c->name;
      k->columns.push_back(c->name);
      t->newKeys.push_back(k);
      return OK;
  }
  extern "C" parseValue constraintName(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      newKeyInfo *k = new newKeyInfo();
      k->name = sql;
      t->newKeys.push_back(k);
      return OK;
  }
  extern "C" parseValue primaryKeys(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      list<newKeyInfo*>::iterator iter = t->newKeys.rbegin();
      newKeyInfo *k;
      if(iter!=t->newKeys.rend()&&(*iter)->type == newKeyInfo::MAX_KEY_TYPE)
          k = (*iter);
      else
      {
          k = new newKeyInfo();
          t->newKeys.push_back(k);
      }
      k->type = newKeyInfo::PRIMARY_KEY;
      return OK;
  }
  extern "C" parseValue uniqueKeys(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      list<newKeyInfo*>::iterator iter = t->newKeys.rbegin();
      newKeyInfo *k;
      if(iter!=t->newKeys.rend()&&(*iter)->type == newKeyInfo::MAX_KEY_TYPE)
          k = (*iter);
      else
      {
          k = new newKeyInfo();
          t->newKeys.push_back(k);
      }
      k->type = newKeyInfo::UNIQUE_KEY;
      return OK;
  }
  extern "C" parseValue uniqueKeyName(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      newKeyInfo *k = *(t->newKeys.rbegin());
      if(!k->name.empty())
              k->name = sql;
      return OK;
  }
  extern "C" parseValue keyColumn(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      newKeyInfo *k = *(t->newKeys.rbegin());
      k->columns.push_back(sql);
      return OK;
  }
  extern "C" parseValue tableCharset(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      t->defaultCharset = sql;
      return OK;
  }
  extern "C" parseValue newTable(handle * h, const string& sql)
  {
      newTableInfo * t = new newTableInfo();
      h->meta.newTables.push_back(t);
      return OK;
  }
  extern "C" parseValue NewTableDBName(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      t->database = sql;
      return OK;
  }
  extern "C" parseValue NewTableName(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      t->table = sql;
      return OK;
  }
  extern "C" parseValue  createTableLike(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      t->createLike = true;
      return OK;
  }
  extern "C" parseValue  NewTableLikedDBName(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      t->likedDatabase = sql;
      return OK;
  }
  extern "C" parseValue  NewTableLikedTableName(handle * h, const string& sql)
  {
      newTableInfo * t = getLastTable(h);
      t->likedTable = sql;
      return OK;
  }
}

