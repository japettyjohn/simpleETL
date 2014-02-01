create database simpleetl;
use simpleetl;
grant all on simpleetl.* to simpleetl@'localhost' identified by 'simpleetl';
flush privileges;


create table keylog {
    table varchar(100),
    name varchar(100),
    value int,
    date_created datetime
}