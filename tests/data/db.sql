CREATE DATABASE testdb;

\c testdb

create table t1(c1 int);
insert into t1 select * from generate_series(1, 1024);

create table t2(c1 int);
insert into t2 select * from generate_series(1, 2048);

create table t3(c1 int);
insert into t3 select * from generate_series(1, 16);