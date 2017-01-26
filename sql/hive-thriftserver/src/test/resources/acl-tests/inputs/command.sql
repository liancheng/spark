create database db1;

create table db1.test using parquet as select * from range(100);

drop table db1.test;

drop database db1;
