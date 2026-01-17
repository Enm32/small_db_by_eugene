This is a simple command line database implemantation. It stores data in tables and supports indexing but does not have primary key functionality.
Sample Queries:
Without Index
create table T1 ( num int, name varchar(9) );
insert into T1 (num, name) values (1, 'Eugene');
insert into T1 (num, name) values (2, 'Mureithi');
select A,B from T1;
select A,B from T1 where A=1;
Output

>
+---+-------+
| num | name    |
+---+-------+
| 1 | Eugene |
| 2 | Mureithi   |
+---+-------+
>
+---+-------+
| num | name    |
+---+-------+
| 1 | Eugene |
+---+-------+
With Index
create table T2 ( A int, B varchar(9) );
create index A_IDX on T2(A);
insert into T2 (A, B) values (1, 'Eugene');
insert into T2 (A, B) values (2, 'Mureithi');
select A,B from T2;
select A,B from T2 where A=1;
>
+---+-------+
| a | b     |
+---+-------+
| 1 | Alice |
| 2 | Bob   |
+---+-------+

> index on A column used
+---+-------+
| a | b     |
+---+-------+
| 1 | Alice |
+---+-------+
NOTE: Delete the tinydb data directory to start fresh.


NOTE:THIS WORK IS BASED OFF OF simpldb BY EDWARD SCIORE
