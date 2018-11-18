# sqlParser
a sql parser
目前只计划支持ddl，不支持dml
目前只有语法树和解析引擎，没有根据语法树产生表结构
2018-11-16
version 0.0.1 
  支持mysql 8.0标准的create table 
2018-11-18
version 0.0.2
  支持alter table [add column]|[change column]|[modify column]|[add index/key]|[drop index/key]
  支持rename table
  支持create index
  支持drop table
  支持drop index
