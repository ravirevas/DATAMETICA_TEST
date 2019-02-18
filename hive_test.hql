hive> select * from data;
OK
1	111	Electronics	Click
2	112	Fashion	Click
3	112	Fashion	Click
1	112	Fashion	Click
2	112	Electronics	Click
3	113	Kids	AddtoCart
4	114	Food	Purchase
5	115	Books	Logout
6	114	Food	Click
7	113	Kids	AddtoCart
8	115	Books	Purchase
9	111	Electronics	Click
10	112	Fashion	Purchase


create table common as SELECT userid FROM data where category=="Fashion" or category=="Electronics" AND action!="Logout" GROUP BY userid HAVING COUNT(*)> 1

--dumping common records in both fashion and electronics to another table called common.
hive> select * from common;
OK
1
2

SET hive.auto.convert.join=false;

select data.userid,data.category from data where data.userid not in (select common.userid from common) and category in ('Fashion','Electronics') and action!="Logout";


10	Fashion
3	Fashion
9	Electronics