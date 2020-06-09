2018-8-18,六,10.3.9.18,钱八,6,error

create table monthlog(logdt string ,week string ,ipaddress string ,name string ,userid string ,status string)
row format delimited fields terminated by ','

load data local inpath '/usr/local/chuzhigao/month/month01.txt' into table monthlog ;

+------------+------------+----------+--+
|  col_name  | data_type  | comment  |
+------------+------------+----------+--+
| logdt      | string     |          |
| week       | string     |          |
| ipaddress  | string     |          |
| name       | string     |          |
| userid     | string     |          |
| status     | string     |          |
+------------+------------+----------+--+



A: 按照星期几统计访问量数据（8分）
   select  week , count(*) as fwl   from monthlog group by week
B: 将原始数据按照用户id、访问日期,访问量进行排序倒序排序（8分）

   select userid , logdt , count(*) as fwl  from monthlog group by userid, logdt order by userid , logdt , fwl desc
C: 统计每个用户的访问量（8分）
    select  userid ,count(*) as fwl  from monthlog group by userid
d: 统计每个状态数据（8分）
   select   status , count(*) as fwl  from monthlog group by status
e: 统计周六和周日每个状态数据
   select  status , count(*) as fwl  from  monthlog  where week in ('六','日') group by status
