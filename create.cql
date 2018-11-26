CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use test;
create table if not exists test.metaattribute (id text, data text, primary key (id));
insert into test.metaattribute (id, data) values ('123', 'huge');
