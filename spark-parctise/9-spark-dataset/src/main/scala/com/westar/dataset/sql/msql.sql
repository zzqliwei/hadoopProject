show databases;
select database();

create database twq;
show databases;

use twq;
show tables;
create table person(name text, age int);
show tables;

desc person;

select * from person;

insert into person(name, age) values ("jeffy", 30);

drop table person;
drop database twq;

show databases;