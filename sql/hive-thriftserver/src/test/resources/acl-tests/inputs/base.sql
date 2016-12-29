-- Create a simple view
create database db1;
create view db1.vw1 as select * from range(10);

-- Select some data as super user.
select * from db1.vw1;

-- Fail selecting data for usr1.
@usr1;
select * from db1.vw1;

-- Grant rights.
@super;
grant select on db1.vw1 to usr1;
grant read_metadata on database db1 to usr1;

-- Succeed in selecting data.
@usr1;
show grant usr1 on db1.vw1;
show tables in db1;
select * from db1.vw1;

-- Cleanup
@super;
drop database db1 cascade;
show databases;
msck repair database __all__ privileges;
