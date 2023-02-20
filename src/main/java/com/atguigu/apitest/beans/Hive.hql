show databases;
create database ld;

create table ld.t_archer(
    id int comment "id 编号",
    name string comment "英雄名称"
)row format delimited
fields terminated by "\t";

select * from t_archer;

create table t_1(
    id int,
    name string,
    age int
)row format delimited
fields terminated by ",";

create table t_2(
    id int,
    name string,
    age int
)row format delimited
fields terminated by ",";

create table t_4(
    id int,
    name string
);
insert into table t_4 select id,name from t_2;

use ld;
drop table if exists t_user;

create table t_user (
    UserId bigint,
    MovieId bigint,
    Rate int,
    Times bigint
)row format delimited
fields terminated by ":";

load data local inpath '/root/hivedata/ratings.dat' into table ld.t_user;

select * from t_user;

----------------
----------------
--example1---t_usa_covid19;
drop table if exists  t_usa_covid19;
create table t_usa_covid19(
    count_data string,
    county string,
    state string,
    fips int,
    cases int,
    deaths int
)row format delimited fields terminated by ",";

load data local inpath '/root/hivedata/us-covid19-counties.dat' into table t_usa_covid19;

select * from t_usa_covid19;
select distinct state from t_usa_covid19;

select county,cases,deaths from t_usa_covid19;

select 1 from t_usa_covid19;

select current_database();

select * from t_usa_covid19 where length(state)>10 and length(county)>10;

select count(county) from t_usa_covid19;

select count(county) as county_int, state  from t_usa_covid19 group by state;

select  max(cases) as county_max,state from t_usa_covid19 group by state;

use ld;
show tables;

select distinct state from t_usa_covid19;

select sum(deaths) as state_death,count(county),state from t_usa_covid19 group by state;

select state as state_death10000,sum(deaths) as death_all from t_usa_covid19 where count_data="2021-01-28" group by state having death_all>10000;

select max(deaths) from t_usa_covid19;

select county ,cases from t_usa_covid19 order by cases;

--Limit
select * from t_usa_covid19 limit 5;

select * from t_usa_covid19 where count_data = "2021-01-28" and state ="California" limit 2,3;


-----select order  from>where>group>having>order>select

----Join------
create table employee(
    id int,
    name string,
    deg string,
    salary int,
    dept string
)row format delimited fields terminated by ",";

create table employee_address(
    id int,
    hno string,
    street string,
    city string
)row format delimited fields terminated by ",";

create table employee_connection (
    id int,
    phno string,
    email string
)row format delimited fields terminated by ",";

load data local inpath "/root/hivedata/employee.txt" into table ld.employee;
load data local inpath "/root/hivedata/employee_address.txt" into table ld.employee_address;
load data local inpath "/root/hivedata/employee_connection.txt" into table ld.employee_connection;

select * from employee_connection limit 5;
select * from employee limit 5;
select * from employee_address limit 5;


select e.id,e.name,e_a.street
from employee e inner join employee_address e_a
on e.id=e_a.id;

select e.id,e_conn.phno,e_conn.email
from employee e right join employee_connection e_conn
on e.id=e_conn.id limit 5;

select e.id,e_conn.phno,e_conn.email
from employee e left join employee_connection e_conn
on e.id=e_conn.id limit 5;

-----show function;
show functions ;
describe function extended count;

select current_date();

select unix_timestamp();

select date_add('2021-02-08',20);
select date_sub('2021-02-08',20);
select datediff('2021-02-08','2020-01-11');

select round(3.313134123324);
select round(3.313134123324,3);

select rand(2);
select rand();


--momo chat data analysis
drop database if  exists db_msg cascade;
create database db_msg;

use db_msg;

create table db_msg.tb_msg_source(
  msg_time             string  comment "消息发送时间"
  , sender_name        string  comment "发送人昵称"
  , sender_account     string  comment "发送人账号"
  , sender_sex         string  comment "发送人性别"
  , sender_ip          string  comment "发送人ip地址"
  , sender_os          string  comment "发送人操作系统"
  , sender_phonetype   string  comment "发送人手机型号"
  , sender_network     string  comment "发送人网络类型"
  , sender_gps         string  comment "发送人的GPS定位"
  , receiver_name      string  comment "接收人昵称"
  , receiver_ip        string  comment "接收人IP"
  , receiver_account   string  comment "接收人账号"
  , receiver_os        string  comment "接收人操作系统"
  , receiver_phonetype string  comment "接收人手机型号"
  , receiver_network   string  comment "接收人网络类型"
  , receiver_gps       string  comment "接收人的GPS定位"
  , receiver_sex       string  comment "接收人性别"
  , msg_type           string  comment "消息类型"
  , distance           string  comment "双方距离"
  , message            string  comment "消息内容"
)row format delimited fields terminated by "\t";

load data local inpath "/root/hivedata/data1.tsv" into  table tb_msg_source;

select * from tb_msg_source limit 10;

set hive.support.concurrency=false;
set hive.support.concurrency=false;
set mapreduce.map.memory.mb=10150;
set mapreduce.reduce.memory.mb=10150;
set mapreduce.reduce.java.opts=-Xmx8120m;

select count(*) from tb_msg_source group by receiver_os;

select count(*) from tb_msg_source;

select count(*) from ld.employee_connection;

select count(*) from tb_msg_source order by sender_sex;

select msg_time from tb_msg_source limit 5;
select substr(msg_time,12,2) from tb_msg_source limit 5;

-- select count(*) as cnt from tb_msg_source;

create table tb_msg_etl as
    select *,
           substr(msg_time,0,10) as dayinfo,
           substr(msg_time,12,2) as hourinfo,
           split(sender_gps,",")[0] as sender_lng,
           split(sender_gps,",")[1] as sender_lat
from tb_msg_source
where length(sender_gps)>0;

select count(*) from tb_msg_etl;
select count(*) from tb_msg_source;

select * from tb_msg_etl limit 5;

desc tb_msg_etl;

use ld;
use db_msg;
select hourinfo,count(*) as total_msg_cnt from tb_msg_etl group by hourinfo;

select * from tb_msg_etl limit 1;
select sender_gps as ip, count(*) as send_nums from tb_msg_etl group by sender_gps;

select count(*) as sender_sum,sender_name from tb_msg_etl group by sender_name,dayinfo
order by  sender_sum desc limit 10;
select count(*) as receiver_sum,receiver_name from tb_msg_etl group by receiver_name,dayinfo
order by  receiver_sum desc limit 10;


show databases ;

use db_msg;

describe formatted db_msg.tb_msg_etl;

show databases ;

create table if not exists tab1(
    col1 int,
    col2 int
)Partitioned by(col3 int)
row format delimited fields terminated by ',';

load data local inpath "/root/hivedata/tab1.txt"   into table tab1 partition(col3=10);
load data local inpath "/root/hivedata/tab1.txt"   into table tab1;

select * from tab1;

insert overwrite directory '/test'  select * from tab1;

show databases ;

use ld
show  tables

set mapreduce.job.reduces;
set mapreduce.job.reduces=2;

use db_msg;
show tables ;
select * from tb_msg_source limit 5;
select * from tb_msg_source cluster by  sender_sex;

set mapreduce.job.reduces=2;
select * from tb_msg_source distribute by sender_sex sort by sender_os limit 10;


select sender_os from tb_msg_source
union   all
select sender_os from tb_msg_etl  limit 10;


select sender_os,sender_sex from (select * from tb_msg_source limit 10)  tab1
union   all
select sender_os,sender_sex from (select * from tb_msg_etl limit 20) tab2   limit  20;

with q1 as(select * from tb_msg_etl),
     q2 as(select * from tb_msg_source)
insert overwrite table tab1
select * from q1
union
select *from q2;

    select * from tb_msg_source limit 2 ,20;

show functions;

describe function +;
describe function extended +;

describe function extended count;

create table tb_test(
    id int
)

load data local inpath '/root/hivedata/tb_test' into table tb_test;
select * from tb_test;

select 1+1  from tb_test;

select 1 from tb_test where "name" like "na_"  --结果是空
select 1 from tb_test where "name" like "na%"   --结果是1
select 1 from tb_test where "name"  not like "na_" --结果是1
select 1 from tb_test where not "name"  like "nam%" --结果是空

select 1 from tb_test where '1231434' rlike '^.*$'
select 1 from tb_test where '1231434' rlike '^.*\\d$' -- \d数字 \转义  末尾为数字
select 1 from tb_test where '1231434' rlike '^\\d+$' --全为数字

select 17 div 3; --取整
select 17 % 3; --取余
select 2 & 8;--2 0010   8 1000  结果为0  0000
--&位运算  按底层二进制操作的结果  与表示两个都为1结果才为1
select 4 & 6;--4 0100   6 0110   结果为4  0100

select 2 ^ 8;--0010 1000   结果10  1010
--^位运算  两者的值不相同 表示的结果才为1

select "l" || "d";  --ld
select concat("hh,","ld");  --hh,ld

--concat
describe function extended concat;
select concat("l","d","hello"); --拼接出 ldhello
select concat_ws(".","www",`array`("ld","com"));  --拼接出 www.ld.com

--substr
select substr("ldhello",-2);    --lo
select substr("ldhello",1,2);  --ld

--regexp_replace    regexp_extract
select regexp_replace('100-200','(\\d+)','num');
--将数字替换为num，\d+ 表示数字无论多少个   结果：num-num
select regexp_extract('100-200','(\\d+)-(\\d+)',2)

describe function extended concat

select split('hello world','\\s');
select split('hello world',':blank:');
select split('hello world',':space:');

select `current_date`();    --显示当前日期
select unix_timestamp();    --显示当前时间戳
select unix_timestamp("2022-11-01 12:02:33");--日期转时间戳
select from_unixtime(3232232231231) --时间戳转日期

select datediff('2022-12-02','2000-11-11')  --日期差
select date_add('2022-12-11',10)       --add date
select date_sub('2022-11-11',22)   --减少日期

select * from tb_msg_source limit;
select count(*) from tb_msg_source;

select case sender_sex when '男' then 'male' else 'female' end from tb_msg_source  limit 20;
--select case 列名 when '初始' then '之后' [when ''  then '']*n  else Other end from TableName;

select nullif(1,2)
select nullif(1,1)

select cast(11 as bigint)
select cast(11 as string)
select cast("ld" as int)

--小写字母变为x，数字变为n，大写字母变为X
select mask("Aabx2134DFF");
--自定义替换
select  mask("Aabx2134DFF",'-','.','^')
select mask_first_n("Aabx2134DFF",4)
select mask_last_n("Aabx2134DFF",4  )


show databases;

select current_database()
show tables;

select distinct * from tb_msg_source limit 2;

describe function extended distinct

show databases;

select parse_url('http://facebook.com/path/p1.php?id=11123&name=ld','HOST')
select parse_url('http://facebook.com/path/p1.php?id=11123&name=ld','QUERY')
select parse_url('http://facebook.com/path/p1.php?id=11123&name=ld','QUERY','name')

create table row2col1(
    col1 string,
    col2 string,
    col3 int
)row format delimited fields terminated by ',';

load data local inpath '/root/hivedata/a.txt' into table row2col1;


select *
from row2col1;

select
    col1 as col1,
    max(case col2 when 'c' then col3 else 0 end)as c,
    max(case col2 when 'd' then col3 else 0 end)as d,
    max(case col2 when 'e' then col3 else 0 end)as e

from
    row2col1
group by col1;

select cast(1 as string)

show databases

create table test1(
    col1 int,
    col2 string,
    col3 string
)row format delimited
    fields terminated by ",";

load data local inpath "/root/hivedata/test01" into table test1;

select * from test1

select
    col2,col3,concat_ws(",",collect_list((cast(col1 as string))))as col1
    from test1
    group by col2,col3;

show databases ;

create database Test;
use Test;
show tables ;

create table cookie(
    name string,
    time_s string,
    value int
)row format delimited fields terminated by ',';

load data inpath '/input/test_row' into table cookie;

select *  from cookie order by value desc limit 4;

select name,time_s,value as value_Biggest
    from (
        select
            name,time_s,value,
            row_number() over (order by value desc) as valueD
    from cookie) as Test
where Test.valueD=1;




create table sub_test(
    Line string
)row format delimited fields terminated by "";

load data local inpath '/root/hivedata/substr.txt' into table sub_test;

select *
from sub_test limit 7;


select line,temperature
from
    (select Line as line,substr(Line,9) as temperature from sub_test)
        as table_tem
order by  temperature desc limit 1;

select * from sub_test;

--每年最高温度
select Year,Date_test,temperature,num
from
    (select substr(Line,1,4) as Year,substr(Line,5,8) as Date_test,substr(Line,9) as temperature,
            row_number() over (partition by substr(Line,1,4) order by substr(Line,9) desc ) as num
     from sub_test )
        as table_tem
where num=1;


--github code
select
  substr(line,1,4) as year,
  max(substr(line,9)) as temperature
from
    sub_test
group by
  substr(line,1,4);


---查询日期和一年的最高温度
---错误的 Hql
-- 在 Group by 子句中，Select 查询的列，要么需要是 Group by 中的列，要么得是用聚合函数（比如 sum、count 等）加工过的列。
-- 不支持直接引用非 Group by 的列。这一点和 MySQL 有所区别
---例如该错误Hql substr(Line,1,4),'-',substr(Line,5,6),'-',substr(Line,7,8)会产生多种情况，因为是在数仓中，数据量极大，
-- hive不可能对不同数据进行笛卡尔积（多对多整合）
select
    concat(substr(Line,1,4),'-',substr(Line,5,6),'-',substr(Line,7,8)) as year_mon_day,max(substr(Line,9)) as tem
    from sub_test
group by  substr(Line,1,4);


--正确代码
select temperature,concat(substr(Line,1,4),'-',substring(Line,5,6),'-',substring(Line,7,8)) as year_mon_day
from
    (select substr(Line,1,4) as Year,substr(Line,5,8) as Date_test,substr(Line,9) as temperature,Line,
            row_number() over (partition by substr(Line,1,4) order by substr(Line,9) desc ) as num
     from sub_test )
        as table_tem
where num=1;


--查找每个爱好中年龄最大的人
create table Favors_tab(
    id int,
    name string,
    age int,
    favors string
)row format delimited fields terminated by ',';

load data local inpath '/root/hivedata/favors_tab' into table Favors_tab;

select * from Favors_tab limit 5;


--使用explode函数  将数组变为侧视图
select Tab_1.* from
    (select id, name, age, laTab.favors,
       row_number() over (partition by laTab.favors order by age desc) as rank_num
from Favors_tab
          lateral view explode(split(favors, "-")) laTab as favors) as Tab_1
where Tab_1.rank_num=1;

show databases;



create table GB_tab(
    dt string,
    id string,
    income int,
    cost int
)row format delimited fields terminated by ",";

load data local inpath '/root/hivedata/GroupBy_test' into table GB_tab;

select * from GB_tab;


show databases

--输出每个产品，在2018年期间，每个月的净利润，日均成本。
select id,year_month,avg(profit) as avg_profit,sum(profit) as sum_profit
from
(select (income - GB_tab.cost) as profit,
        id,
        substr(dt, 1, 7) as year_month
 from GB_tab) as year_month_Tab
group by id,year_month;



show databases ;
--输出每个产品，在2018年3月中每一天与上一天相比，成本的变化。

---每个子查询必须给查询取别名
select *,(cost-last_cost) as del_cost
from
(select
    id,cost,dt,
       substr(dt,6,2) as dt_month,
       --取this.cost前一个cost,前面无cost的话,赋值默认的0,按照月份和id进行分区，按照日期和id排序
       lag(cost,1,0) over (partition by substr(dt,6,2),id order by id,dt) as last_cost
from GB_tab
where substr(dt,6,2)='03') as tab;


--left join
select aa.aid as id,aa.bday as day,(aa.bcost-aa.acost) as difference
from
  (
  select a.id as aid,a.cost as acost,b.cost as bcost,b.day as bday from
    (
    select id,income,cost,substring(dt,1,4) as year,substring(dt,6,2) as month,substring(dt,9,2) as day
    from GB_tab
    --过滤不满足的
    where substring(dt,6,2)='03' and substring(dt,1,4)='2018'
    order by id,month,day
    ) a
  left join
    (
    select id,income,cost,substring(dt,1,4) as year,substring(dt,6,2) as month,substring(dt,9,2) as day
    from GB_tab
    where substring(dt,6,2)='03' and substring(dt,1,4)='2018'
    order by id,month,day
    ) b
    --根据题目要求前一天的来连接
  on a.id=b.id and a.month=b.month and a.day=b.day-1
  ) aa
where aa.bcost is not null;


--输出2018年4月，有多少个产品总收入大于22000元，必须用一句SQL语句实现，且不允许使用关联表查询、子查询。

select id,sum(income) as sum_income
from GB_tab
where substr(dt,1,7)='2018-04'
group by id
--想要查询加上日期  必须在group by中增加根据日期分组
having sum_income>=22000

select id,sum(income) as sum_income,substr(dt,1,7) as year_month
from GB_tab
where substr(dt,1,7)='2018-04'
group by id,substr(dt,1,7)
--想要查询加上日期  必须在group by中增加根据日期分组
having sum_income>=22000


select * from GB_tab;
--输出2018年4月，总收入最高的那个产品，每日的收入，成本，过程使用over()函数
select a.id as id,dt,income,cost
    from
(select id  --这个表得出总收入最高的产品id
    from
    (select id,sum(income) as sum_income,row_number() over (order by sum(income) desc ) as num_income
        from GB_tab
    where substr(dt,1,7)='2018-04'
group by id) as tab
where num_income=1) a
    left join    --left join
(select *
    from GB_tab
where
    substr(dt,1,7)='2018-04'
) b
on  a.id=b.id;



-- RANK() 排序相同时会重复，总数不会变
-- DENSE_RANK() 排序相同时会重复，总数会减少
-- ROW_NUMBER() 会根据顺序计算
-- 这三个函数常常和开窗函数结合在一起使用
--使用sum() over(),rank() over()
select dt,id,income,cost,sum_income
    from
(select *,
       rank() over (partition by month_dt order by sum_income desc ) as num_rank
from
    (select *,
            substr(dt,6,2) as month_dt,
        sum(income) over(partition by id) as sum_income
        from
        GB_tab
        where substr(dt,1,7)='2018-04') tab_1
)tab_2
where tab_2.num_rank=1
    order by dt desc;   --通过日期降序排列表

show databases;
drop table if exists idNumber;

create table if not exists idNumber(
    id int,
    number string
)row format delimited fields terminated by ",";

load data local inpath "/root/hivedata/id_number" into table idNumber;
select * from idNumber order by id asc;



---join on
select a.id
    from idNumber a join idNumber b on a.id=b.id-1
        join idNumber c on a.id=c.id-2
where a.number=b.number
and a.number=c.number;

--窗口函数
--lag() lead()
select (id-1) as id
from
(select id,
     lag(number,1) over (order by id) as previous_num,
        number as mid_num,
     lead(number,1) over (order by id) as next_num
from idNumber
) as tab
where
    tab.mid_num=tab.next_num and tab.mid_num=tab.previous_num;

