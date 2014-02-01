create database csietl;
use csietl;
grant all on csietl.* to csietl@'localhost' identified by 'csietl';
flush privileges;


create table keylog {
    table varchar(100),
    name varchar(100),
    value int,
    date_created datetime
}