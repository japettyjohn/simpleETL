create database simpleetl;
use simpleetl;
grant all on simpleetl.* to simpleetl@'localhost' identified by 'simpleetl';
flush privileges;


create table keylog (
    tableName varchar(100)  not null,
    name varchar(255)  not null,
    value bigint  not null,
    date_created datetime  not null,
    KEY `keylog_tnn` (`tableName`,`name`)
);

create table keylog_last (
	tableName varchar(255) not null,
	name varchar(255) not null,
	value bigint not null,
	date_created datetime not null,
	primary key(`tableName`,`name`)
);

create table auditlog (
	id int not null auto_increment,
	tableName varchar(255) not null,
	colName varchar(255) not null,
	value bigint not null,
	date_created datetime  not null,
	primary key(id),
	KEY `auditlog_tc` (`tableName`,`colName`)
);


