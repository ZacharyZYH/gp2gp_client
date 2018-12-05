CREATE DATABASE testdb;

\c testdb

create table t1(i int);
insert into t1 select * from generate_series(1, 1024);