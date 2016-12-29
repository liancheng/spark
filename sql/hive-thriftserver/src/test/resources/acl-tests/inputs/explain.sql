create database perm;
create table perm.t1(a int, p int);
create table perm.t2(a int, p int);
create function perm.fn1 AS 'org.apache.hadoop.hive.ql.udf.UDFToString';

create or replace view perm.vw1 as
select t1.a, t2.p from perm.t1, perm.t2 where t1.p = t2.p;

use perm;

@usr1;
use perm;
explain select fn1(a) from perm.t1;
explain select fn1(t1.a), t2.* from perm.t1, perm.t2 where t1.p = t2.p;
explain select * from perm.vw1;

@super;
grant read_metadata on perm.vw1 to usr1;

@usr1;
explain select * from perm.vw1;

@super;
grant select on anonymous function to usr1;
grant read_metadata on perm.t1 to usr1;
grant select on perm.t1 to usr1;
grant select on perm.t2 to usr1;

@usr1;
explain select fn1(a) from perm.t1;
select fn1(t1.a), t2.* from perm.t1, perm.t2 where t1.p = t2.p;
explain select fn1(t1.a), t2.* from perm.t1, perm.t2 where t1.p = t2.p;

@super;
grant read_metadata on perm.t2 to U;

@usr;
explain select fn1(t1.a), t2.* from perm.t1, perm.t2 where t1.p = t2.p;

@super;
use default;
drop temp function fn1;
drop database perm cascade;
msck repair database __all__ privileges;
