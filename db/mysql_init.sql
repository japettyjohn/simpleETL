create database simpleetl;
use simpleetl;
grant all on simpleetl.* to simpleetl@'localhost' identified by 'simpleetl';
flush privileges;


create table keylog (
    tableName varchar(100),
    name varchar(255),
    value bigint,
    date_created datetime,
    KEY `keylog_tnn` (`tableName`,`name`)
)
