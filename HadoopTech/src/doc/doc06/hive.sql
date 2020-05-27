



















create table test(name string,sex int,height int ,classname string)
row format delimited fields terminated by '\t'

insert into test values('chuzhigao',1,172,'1712e')

'chuzhigao'   1   172 '1712e'

//从本地目录将数据导入hive
load data local inpath '/usr/local/chuzhigao/test526.txt' into table test

load data inpath '/chuzhigao/hivetest/test526.txt' into table test


.然后创建名字为result的表 （8分）
             字段如下：
                cid      String
                avgscore String

create table result(cid String , avgscore String)
row format delimited fields terminated by '\t'


load data inpath '/yuekao/result3/part-r-00000' into table result



4.2


/yuekao/result3/part-r-00000

 ./hadoop fs -cat /yuekao/result3/part-r-00000




hadoop jar /opt/module/hadoop-2.7.2/share/hadoop/mapreduce/mothtest02-1.0-SNAPSHOT.jar com.bawei.test2.AvgScoreDriver /yuekao/source1 /yuekao/result3




hadoop jar hadoop-e.jar FileCopyWithProgress sample.txt�0�2hdfs://localhost/user/sample_one.txt



2015-08-07    136202741    255.123.123.111
2015-08-07    136202123    255.123.124.111
2015-08-07    136202765    255.123.125.111
2015-06-12    136202741    255.123.123.111
2015-06-12    136202982    255.123.122.111
2015-06-12    136202234    255.123.126.111
2015-11-06    136202764    255.123.121.111
2015-11-06    136202234    255.123.129.111








create table logininfo(telephone string,ipaddress string)
partitioned by(logdt date)
row format delimited fields terminated by '\t'















// 登录日期    登录账号        登录IP
create table logininfo(userid String , ip String)
partitioned by (logdt String)
row  format  delimited fields terminated by '\t' ;


load data local inpath  '/usr/local/chuzhigao/tets01.txt'  into table logininfo partition(logdt='2015-08-07')
load data local inpath  '/usr/local/chuzhigao/test02.txt'  into table  logininfo partition(logdt='2015-06-12')



 136202741    255.123.123.111
 136202123    255.123.124.111
 136202765    255.123.125.111

 136202741    255.123.123.111
 136202982    255.123.122.111
 136202234    255.123.126.111

 36202764    255.123.121.111
 36202234    255.123.129.111

select  max() avg() min() sum from b

 load data local '' into table cc partition(logdt='')



删除表 drop table logininfo

load data local inpath '/usr/local/chuzhigao/tets01.txt' into table logininfo partition(logdt='2015-08-07');
load data local inpath '/usr/local/chuzhigao/test02.txt' into table logininfo partition(logdt='2015-06-12');
load data local inpath '/usr/local/chuzhigao/test03.txt' into table logininfo partition(logdt='2015-11-06');

load data local inpath '/opt/module/datas/dept.txt' into table default.dept_partition partition(month='201709')


add jar /opt/module/jars/testudf.jar


create temporary function cc  as "com.bawei.GetArea"
    create  function cc  as "com.bawei.GetArea"

    查询2015-06-12分区对应的所有数据，信息包括：日期，账号，IP,城市

    select logdt , userid , ip , getArea1(ip) from logininfo where  logdt ='2015-06-12'


 create temporary function mylower as "com.atguigu.hive.Lower";


create temporary function mylower as "com.atguigu.hive.Lower";

// ID    编号    检查时间点    评分
 //  id   sn      checkdate   score


 create table t (id int , sn String)
     row format delimited fields terminated by ''

 create table t_chktable(id int , sn String , checkdate String , score int )
 row format delimited fields terminated by ' ';


 load data local inpath "/usr/local/chuzhigao/part-r-00000" into table t_chktable

 4)    使用SQL按ID分组统计每个ID的检查次数，并按照检查次数倒序排序。（6分）

    select count(*) from t_chktable where id ='1004'

     select  id, count(*) as cknum   from t_chktable group by id  order by cknum desc ;

5)    使用SQL统计出检查次数最多的编号。（6分）

select a.sn from ( select  sn , count(*) as cknum   from t_chktable group by sn   order by cknum desc  limit 1 ) a


6)    使用SQL统计出各编号的检查平均分数，并按照分数正序排序。（6分）
      select  sn , round(avg(score),2) as pjfs  from t_chktable group by sn  order by pjfs asc


7)    使用SQL统计出分数最高的五条记录并按照分数倒序排序（6分）

     select   *   from t_chktable  order by score desc limit 5


     第二体
     表一存储的是学生编号、学生姓名；表二存储的是学生编号、考试科目、考试成绩

     create table student(stuno String ,stdname String)
     row format delimited fields terminated by '\t'

     create table stdscore(stuno String , classname String , score int )
     row format delimited fields terminated by '\t'

select xs.stdname , concat( fs.classname , "=",fs.score )  from stdscore fs , student xs
where  fs.stuno = xs.stuno

 load data local inpath "/usr/local/chuzhigao/score.txt" into table stdscore
 load data local inpath "/usr/local/chuzhigao/student.txt" into table student

用hive sql 求的过程

select xs.stdname , concat( fs.classname , "=",fs.score )  from stdscore fs , student xs
                > where  fs.stuno = xs.stuno;




row format delimited
fields terminated by ',';

create table t_b(name string,nick string)
row format delimited
fields terminated by ',';

load data local inpath '/root/hivetest/a.txt' into table t_a;
load data local inpath '/root/hivetest/b.txt' into table t_b;




t_a
a,1
b,2
c,3

t_b
b,22
c,33
d,44

create table t_a(name string, age int)
row format delimited fields terminated by ''




select a.* , b.* from t_a a right join t_b b on a.name = b.name
select a.* , b.* from t_a a, t_b b  where a.name = b.name



create table t_a(name string,age int)
row format delimited fields terminated by ','

insert into t_a values('a',1);
insert into t_a values('b',2);
insert into t_a values('c',3);

insert into t_b values('b',22);
insert into t_b values('c',33);
insert into t_b values('d',44);

select a.*, b.* from t_a a , t_b b where a.name = b.name

select a.*, b.* from t_a a  join  t_b b  on  a.name = b.name

select a.* ,b.* from t_a a left join t_b b on a.name = b.name

select a.* ,b.* from t_a a right join t_b b on a.name = b.name


用左链接 去实现

select a.*, b.* from t_a  a left join t_b b on a.name = b.name


create table t_b(name string, age int)
row format delimited  fields terminated by ','
-- 各类join
--1/ 内连接
-- 笛卡尔积
select a.* , b.* from t_a a , t_b b

select a.* , b.* from  t_a  a join t_b b on a.name = b.name





select a.*,b.*
from t_a a inner join t_b b;


select a.*, b.* from t_a a , t_b  b

select a.* , b.* from t_a a inner join t_b b ;



select a.*, b.* from t_a  a   full join  t_b b on a.name = b.name


-- 指定join条件
select a.*,b.*
from
t_a a join t_b b on a.name=b.name;

-- 2/ 左外连接（左连接）
select a.*,b.*
from
t_a a left outer join t_b b on a.name=b.name;


-- 3/ 右外连接（右连接）
select a.*,b.*
from
t_a a right outer join t_b b on a.name=b.name;

-- 4/ 全外连接
select a.*,b.*
from
t_a a full outer join t_b b on a.name=b.name;


-- 5/ 左半连接
select a.*
from
t_a a left semi join t_b b on a.name=b.name;


-- 分组聚合查询

-- 针对每一行进行运算
select ip,upper(url),access_time  -- 该表达式是对数据中的每一行进行逐行运算
from t_pv_log;

-- 求每条URL的访问总次数

select url,count(1) as cnts   -- 该表达式是对分好组的数据进行逐组运算
from t_pv_log
group by url;

-- 求每个URL的访问者中ip地址最大的

select url,max(ip)
from t_pv_log
group by url;

-- 求每个用户访问同一个页面的所有记录中，时间最晚的一条

select ip,url,max(access_time)
from  t_pv_log
group by ip,url;


create table t_pv_log1(ip string, url string,access_time string)
row format delimited fields terminated by ','

create table t_a(name string ,age int)
row format delimited fields terminated by ',';

 按

 load data local inpath ''
 load data local inpath '/usr/local/chuzhigao/tv.txt'  into table t_pv_log1


select  a.ip, count(*) as number_login  from t_pv_log1 a  group by a.ip
order by number_login desc
limit 5


select * from (
select   t.url ,  t.ip, count(*) as djl from t_pv_log1 t
group by t.url , t.ip
order by djl desc
) t
where  t.djl >1

select   t.url ,  t.ip, count(*) as djl from t_pv_log1 t
group by t.url , t.ip
having  djl >1
order by djl desc





-- 分组聚合综合示例
-- 有如下数据
/*
192.168.33.3,http://www.edu360.cn/stu,2017-08-04 15:30:20
192.168.33.3,http://www.edu360.cn/teach,2017-08-04 15:35:20
192.168.33.4,http://www.edu360.cn/stu,2017-08-04 15:30:20

192.168.33.4,http://www.edu360.cn/job,2017-08-04 16:30:20
192.168.33.5,http://www.edu360.cn/job,2017-08-04 15:40:20


192.168.33.3,http://www.edu360.cn/stu,2017-08-05 15:30:20
192.168.44.3,http://www.edu360.cn/teach,2017-08-05 15:35:20
192.168.33.44,http://www.edu360.cn/stu,2017-08-05 15:30:20
192.168.33.46,http://www.edu360.cn/job,2017-08-05 16:30:20
192.168.33.55,http://www.edu360.cn/job,2017-08-05 15:40:20


192.168.133.3,http://www.edu360.cn/register,2017-08-06 15:30:20
192.168.111.3,http://www.edu360.cn/register,2017-08-06 15:35:20
192.168.34.44,http://www.edu360.cn/pay,2017-08-06 15:30:20
192.168.33.46,http://www.edu360.cn/excersize,2017-08-06 16:30:20
192.168.33.55,http://www.edu360.cn/job,2017-08-06 15:40:20
192.168.33.46,http://www.edu360.cn/excersize,2017-08-06 16:30:20
192.168.33.25,http://www.edu360.cn/job,2017-08-06 15:40:20
192.168.33.36,http://www.edu360.cn/excersize,2017-08-06 16:30:20
192.168.33.55,http://www.edu360.cn/job,2017-08-06 15:40:20

*/
-- 建表映射上述数据
create table t_access(ip string,url string,access_time string)
partitioned by (dt string)
row format delimited fields terminated by ',';


-- 导入数据
load data local inpath '/root/hivetest/access.log.0804' into table t_access partition(dt='2017-08-04');
load data local inpath '/root/hivetest/access.log.0805' into table t_access partition(dt='2017-08-05');
load data local inpath '/root/hivetest/access.log.0806' into table t_access partition(dt='2017-08-06');

load data local inpath '/root/hive/visit.data' into table t_visit_video partition(day='20180516');

load data local inpath '/access.log.0804' into table t_access partition(dt='2017-08-04');


-- 查看表的分区
show partitions t_access;

-- 求8月4号以后，每天http://www.edu360.cn/job的总访问次数，及访问者中ip地址中最大的
select dt,'http://www.edu360.cn/job',count(1),max(ip)
from t_access
where url='http://www.edu360.cn/job'
group by dt having dt>'2017-08-04';


select dt,max(url),count(1),max(ip)
from t_access
where url='http://www.edu360.cn/job'
group by dt having dt>'2017-08-04';


select dt,url,count(1),max(ip)
from t_access
where url='http://www.edu360.cn/job'
group by dt,url having dt>'2017-08-04';



select dt,url,count(1),max(ip)
from t_access
where url='http://www.edu360.cn/job' and dt>'2017-08-04'
group by dt,url;


-- 求8月4号以后，每天每个页面的总访问次数，及访问者中ip地址中最大的

select dt,url,count(1),max(ip)
from t_access
where dt>'2017-08-04'
group by dt,url;

-- 求8月4号以后，每天每个页面的总访问次数，及访问者中ip地址中最大的，且，只查询出总访问次数>2 的记录
-- 方式1：
select dt,url,count(1) as cnts,max(ip)
from t_access
where dt>'2017-08-04'
group by dt,url having cnts>2;


-- 方式2：用子查询
select dt,url,cnts,max_ip
from
(select dt,url,count(1) as cnts,max(ip) as max_ip
from t_access
where dt>'2017-08-04'
group by dt,url) tmp
where cnts>2;


+----------------+---------------------------------+-----------------------+--------------+--+
|  t_access.ip   |          t_access.url           | t_access.access_time  | t_access.dt  |
+----------------+---------------------------------+-----------------------+--------------+--+

| 192.168.33.46  | http://www.edu360.cn/job        | 2017-08-05 16:30:20   | 2017-08-05   |
| 192.168.33.55  | http://www.edu360.cn/job        | 2017-08-05 15:40:20   | 2017-08-05   |

| 192.168.33.55  | http://www.edu360.cn/job        | 2017-08-06 15:40:20   | 2017-08-06   |
| 192.168.33.25  | http://www.edu360.cn/job        | 2017-08-06 15:40:20   | 2017-08-06   |
| 192.168.33.55  | http://www.edu360.cn/job        | 2017-08-06 15:40:20   | 2017-08-06   |
+----------------+---------------------------------+-----------------------+--------------+



load data local inpath '/usr/local/chuzhigao/test527.txt' into table test

create table test(
name string,
friends array<string>,
children map<string, int>,
address struct<street:string, city:string>
)
row format delimited fields terminated by ','
collection items terminated by '_'
map keys terminated by ':'
lines terminated by '\n';



/*
HIVE 中的复合数据类型

*/
-- 数组
-- 有如下数据：
战狼2,吴京:吴刚:龙母,2017-08-16
三生三世十里桃花,刘亦菲:痒痒,2017-08-20
普罗米修斯,苍老师:小泽老师:波多老师,2017-09-17
美女与野兽,吴刚:加藤鹰,2017-09-17

create table t_movie(movie_name string , actors array<string> ,first_show date)
row format delimited fields terminated by ','
collection items terminated by ':'


load data local inpath '/usr/local/chuzhigao/movie.txt' into table t_movie


-- 建表映射：
create table t_movie(movie_name string,actors array<string>,first_show date)
row format delimited fields terminated by ','
collection items terminated by ':';

-- 导入数据
load data local inpath '/root/hivetest/actor.dat' into table t_movie;
load data local inpath '/root/hivetest/actor.dat.2' into table t_movie;


 max min avg array_contains,
-- 查询
select movie_name,actors[1],first_show from t_movie;

select * from  t_movie a where array_contains(a.actors,'吴刚')

select movie_name,actors,first_show
from t_movie where array_contains(actors,'吴刚');

select movie_name
,size(actors) as actor_number
,first_show
from t_movie;



-- 有如下数据：
1,zhangsan,father:xiaoming#mother:xiaohuang#brother:xiaoxu,28
2,lisi,father:mayun#mother:huangyi#brother:guanyu,22
3,wangwu,father:wangjianlin#mother:ruhua#sister:jingtian,29
4,mayun,father:mayongzhen#mother:angelababy,26


create table t_family(id int, name string, family_members map<string,string>,age int)
row format delimited fields terminated by ','
collection items terminated by '#'
map keys terminated by ':'


load data inpath '/yuekao/result/fm.dat' into table t_family

-- 建表映射上述数据
create table t_family(id int,name string,family_members map<string,string>,age int)
row format delimited fields terminated by ','
collection items terminated by '#'
map keys terminated by ':';

-- 导入数据
load data local inpath '/root/hivetest/fm.dat' into table t_family;

+--------------+----------------+----------------------------------------------------------------+---------------+--+
| t_family.id  | t_family.name  |                    t_family.family_members                     | t_family.age  |
+--------------+----------------+----------------------------------------------------------------+---------------+--+
| 1            | zhangsan       | {"father":"xiaoming","mother":"xiaohuang","brother":"xiaoxu"}  | 28            |
| 2            | lisi           | {"father":"mayun","mother":"huangyi","brother":"guanyu"}       | 22            |
| 3            | wangwu         | {"father":"wangjianlin","mother":"ruhua","sister":"jingtian"}  | 29            |
| 4            | mayun          | {"father":"mayongzhen","mother":"angelababy"}                  | 26            |
+--------------+----------------+----------------------------------------------------------------+---------------+--+

map(key,value)


-- 查出每个人的 爸爸、姐妹
select id,name,family_members["father"] as father,family_members["sister"] as sister,age
from t_family;

-- 查出每个人有哪些亲属关系

select id,name,map_keys(family_members) as relations, map_values(family_members) as relations,age
from  t_family;

-- 查出每个人的亲人名字
select id,name,map_values(family_members) as relations,age
from  t_family;

-- 查出每个人的亲人数量
select id,name,size(family_members) as relations,age
from  t_family;

-- 查出所有拥有兄弟的人及他的兄弟是谁
-- 方案1：一句话写完
select id,name,age,family_members['brother']
from t_family  where array_contains(map_keys(family_members),'brother');


-- 方案2：子查询
select id,name,age,family_members['brother']
from
(select id,name,age,map_keys(family_members) as relations,family_members
from t_family) tmp
where array_contains(relations,'brother');


/*  hive数据类型struct

假如有以下数据：
1,zhangsan,18:male:深圳
2,lisi,28:female:北京
3,wangwu,38:male:广州
4,赵六,26:female:上海
5,钱琪,35:male:杭州
6,王八,48:female:南京
*/

-- 建表映射上述数据

drop table if exists t_user;
create table t_user(id int,name string,info struct<age:int,sex:string,addr:string>)
row format delimited fields terminated by ','
collection items terminated by ':';

-- 导入数据
load data local inpath '/root/hivetest/user.dat' into table t_user;

-- 查询每个人的id name和地址
select id,name,info.addr
from t_user;

 1/ HIVE是什么？
HIVE是一个可以将sql翻译为MR程序的工具
HIVE支持用户将HDFS上的文件映射为表结构，然后用户就可以输入SQL对这些表（HDFS上的文件）进行查询分析
HIVE将用户定义的库、表结构等信息存储hive的元数据库（可以是本地derby，也可以是远程mysql）中



2/ HIVE的用途？
解放大数据分析程序员，不用自己写大量的mr程序来分析数据，只需要写sql脚本即可
HIVE可用于构建大数据体系下的数据仓库


3/ HIVE的使用方式？
方式1：可以交互式查询：
  **  bin/hive   ----->    hive>select * from t_test;
  ** 将hive启动为一个服务：  bin/hiveserver   ，然后可以在任意一台机器上使用beeline客户端连接hive服务，进行交互式查询

方式2：可以将hive作为命令一次性运行：
  **  bin/hive -e "sql1;sql2;sql3;sql4"
  **  事先将sql语句写入一个文件比如 q.hql ，然后用hive命令执行：　　bin/hive -f q.hql


方式3：可以将方式2写入一个xxx.sh脚本中



4/ HIVE的DDL语法
建库：    create database db1;   ---> hive就会在/user/hive/warehouse/下建一个文件夹： db1.db
建内部表：    use db1;
create table t_test1(id int,name string,age int,create_time bigint)
row format delimited
fields terminated by '\001';

      建表后，hive会在仓库目录中建一个表目录：  /user/hive/warehouse/db1.db/t_test1


建外部表：
create external table t_test1(id int,name string,age int,create_time bigint)
row format delimited
fields terminated by '\001'
location '/external/t_test';


导入数据：
本质上就是把数据文件放入表目录；
可以用hive命令来做：
hive>  load data [local] inpath '/data/path' [overwrite] into table t_test;



**建分区表：
分区的意义在于可以将数据分子目录存储，以便于查询时让数据读取范围更精准；
create table t_test1(id int,name string,age int,create_time bigint)
partitioned by (day string,country string)
row format delimited
fields terminated by '\001';


插入数据到指定分区：
hive> load data [local] inpath '/data/path1' [overwrite] into table t_test partition(day='2017-06-04',country='China');
hive> load data [local] inpath '/data/path2' [overwrite] into table t_test partition(day='2017-06-05',country='China');
hive> load data [local] inpath '/data/path3' [overwrite] into table t_test partition(day='2017-06-04',country='England');

导入完成后，形成的目录结构如下：
/user/hive/warehouse/db1.db/t_test1/day=2017-06-04/country=China/...
/user/hive/warehouse/db1.db/t_test1/day=2017-06-04/country=England/...
/user/hive/warehouse/db1.db/t_test1/day=2017-06-05/country=China/...



表定义的修改：改表名、增加列，删除列，修改列定义.......


5/ HIVE的DML

基本查询语法跟标准sql基本一致
SELECT FIELDS,FUNCTION(FIELDS)
FROM T1
JOIN T2
WHERE CONDITION
GROUP BY FILEDS
HAVING CONDTION
ORDER BY FIELDS DESC|ASC

各类JOIN语法跟SQL也基本一致，不过HIVE有一个自己的特别的JOIN：  LEFT SEMI JOIN
hive在1.2.0之前不支持“不等值”join，但在1.2.0后支持不等值join，只是语法必须按如下形式写：

SELECT A.*,B.* from A,B WHERE A.ID>B.ID;



各类流程控制语句根SQL也基本一致：
  case when l.userid is null
       then concat('hive',rand())
       when l.userid > 20
       then concat('hive',rand())
       else l.userid




6/ HIVE的内置函数

时间处理函数：
from_unixtime(21938792183,'yyyy-MM-dd HH:mm:ss')  -->   '2017-06-03 17:50:30'

类型转换函数：
from_unixtime(cast('21938792183' as bigint),'yyyy-MM-dd HH:mm:ss')

字符串截取和拼接
substr("abcd",1,3)  -->   'abc'
concat('abc','def')  -->  'abcdef'

Json数据解析函数
get_json_object('{\"key1\":3333，\"key2\":4444}' , '$.key1')  -->  3333

json_tuple('{\"key1\":3333，\"key2\":4444}','key1','key2') as(key1,key2)  --> 3333, 4444

url解析函数
parse_url_tuple('http://www.edu360.cn/bigdata/baoming?userid=8888','HOST','PATH','QUERY','QUERY:userid')
--->     www.edu360.cn      /bigdata/baoming     userid=8888   8888


*******  函数：explode  和 lateral view
可以将一个数组变成列




加入有一个表，其中的字段为array类型
表数据：
1,zhangsan,数学:语文:英语:生物
2,lisi,数学:语文
3,wangwu,化学:计算机:java编程

建表：
create table t_xuanxiu(uid string,name string,kc array<string>)
row format delimited
fields terminated by ','
collection items terminated by ':';


** explode效果示例：
select explode(kc) from t_xuanxiu where uid=1;
数学
语文
英语
生物


** lateral view 表生成函数
hive> select uid,name,tmp.* from t_xuanxiu
    > lateral view explode(kc) tmp as course;
OK
1       zhangsan        数学
1       zhangsan        语文
1       zhangsan        英语
1       zhangsan        生物
2       lisi    数学
2       lisi    语文
3       wangwu  化学
3       wangwu  计算机
3       wangwu  java编程



 ==== 利用explode和lateral view 实现hive版的wordcount
 有以下数据：
a b c d e f g
a b c
e f g a
b c d b

对数据建表：
create table t_juzi(line string) row format delimited;

导入数据：
load data local inpath '/root/words.txt' into table t_juzi;

** ***** ******** ***** ******** ***** ******** wordcount查询语句：***** ******** ***** ******** ***** ********
select a.word,count(1) cnt
from
(select tmp.* from t_juzi lateral view explode(split(line,' ')) tmp as word) a
group by a.word
order by cnt desc;


***************** ******** ***** ******** 　row_number()  over() 函数***** ******** ***** ******** ***** ******** ***** ******** ***** *******
常用于求分组TOPN


create table t_rowtest1(name string, kcId string , score int)
row format delimited fields terminated by ','

load data local inpath '/usr/local/chuzhigao/t_rowtest1.txt' into table t_rowtest1


/// row_number 不会发送跳跃

select * from (
select *, row_number() over(partition by name order by score desc ) as paiming   from  t_rowtest1
) a
where a.paiming <=2

//rank 相同排名会把下一个的排名数据给挤掉，会发送跳跃

select * from (
select *, rank() over(partition by name order by score desc ) as paiming   from  t_rowtest1
) a
where a.paiming <=2



//任何有相同排名情况下的数据。相同排名排名 不会挤掉下一个排名

select * from (
select *, DENSE_RANK() over(partition by name order by score desc ) as paiming   from  t_rowtest1
) a
where a.paiming <=2

这个 与以前的完全不一样分割，将数据可以按几个给定额分区进行数据的排序，

select *, NTILE(3) over( partition by name  order by score desc ) as paiming   from  t_rowtest1
这里的第一个参数是 数将是按名字进行分区排序，可以制定排序的总的排序个数
+------------------+------------------+-------------------+----------+--+
| t_rowtest1.name  | t_rowtest1.kcid  | t_rowtest1.score  | paiming  |
+------------------+------------------+-------------------+----------+--+
| lisi             | kc6              | 98                | 1        |
| lisi             | kc3              | 98                | 1        |
| lisi             | kc2              | 95                | 2        |
| lisi             | kc1              | 88                | 2        |
| lisi             | kc4              | 48                | 3        |
| lisi             | kc5              | 38                | 3        |
| zhangsan         | kc7              | 95                | 1        |
| zhangsan         | kc2              | 95                | 1        |
| zhangsan         | kc1              | 90                | 1        |
| zhangsan         | kc6              | 90                | 2        |
| zhangsan         | kc3              | 58                | 2        |
| zhangsan         | kc4              | 48                | 3        |
| zhangsan         | kc5              | 38                | 3        |
+------------------+------------------+-------------------+----------+-











select *,row_number() over(partition by name order by score desc) as rank from t_rowtest;


有如下数据：
zhangsan,kc1,90
zhangsan,kc6,90
zhangsan,kc2,95
zhangsan,kc7,95
zhangsan,kc3,58
zhangsan,kc4,48
zhangsan,kc5,38
lisi,kc1,88
lisi,kc2,95
lisi,kc3,98
lisi,kc6,98
lisi,kc4,48
lisi,kc5,38


lisi,kc3,98
lisi,kc2,95

zhangsan,kc2,95
zhangsan,kc1,90


top n  按姓名进行分组，求各个人前面排名分数最高的2门科目


select a.name , max(a.score) from t_rowtest a group by  a.name order by a.score desc limit 2

| name      | string     |          |
| kcid      | string     |          |
| score     | int        |          |
+-----------+------------+----------+-



select a.name , max(a.score)  as fs from t_rowtest a group by  a.name order by  fs desc limit 2

load data local inpath '/usr/local/chuzhigao/t_rowtest.txt' into table t_rowtest


create table t_rowtest(name string, kcId string , score int)
row format delimited fields terminated by ','


建表：
create table t_rowtest(name string,kcId string,score int)
row format delimited
fields terminated by ',';

导入数据：

利用row_number() over() 函数看下效果：
select *,row_number() over(partition by name order by score desc) as rank from t_rowtest;
+-----------------+-----------------+------------------+----------------------+--+
| t_rowtest.name  | t_rowtest.kcid  | t_rowtest.score  | rank                 |
+-----------------+-----------------+------------------+----------------------+--+
| lisi            | kc3               | 98               | 1                    |
| lisi            | kc2               | 95               | 2                    |
| lisi            | kc1               | 88               | 3                    |
| zhangsan        | kc2               | 95               | 1                    |
| zhangsan        | kc1               | 90               | 2                    |
| zhangsan        | kc3               | 68               | 3                    |
+-----------------+-----------------+------------------+----------------------+--+

select * from (
select  *, row_number() over(partition by name order by score desc ) as paiming    from t_rowtest
) a
where a.paiming <=2





从而，求分组topn就变得很简单了：


select name,kcid,score
from
(select *,row_number() over(partition by name order by score desc) as rank from t_rowtest) tmp
where rank<3;
+-----------+-------+--------+--+
|   name    | kcid  | score  |
+-----------+-------+--------+--+
| lisi      | kc3     | 98     |
| lisi      | kc2     | 95     |
| zhangsan  | kc2     | 95     |
| zhangsan  | kc1     | 90     |
+-----------+-------+--------+--+


create table t_rate_topn_uid
as
select uid,movie,rate,ts
from
(select *,row_number() over(partition by uid order by rate desc) as rank from t_rate) tmp
where rank<11;



7/ 自定义函数   ***** ******** ***** ******** ***** ******** ***** ********

有如下数据：
1,zhangsan:18-1999063117:30:00-beijing
2,lisi:28-1989063117:30:00-shanghai
3,wangwu:20-1997063117:30:00-tieling

建表：
create table t_user_info(info string)
row format delimited;

导入数据：
load data local inpath '/root/udftest.data' into table t_user_info;

需求：利用上表生成如下新表t_user：
uid,uname,age,birthday,address

思路：可以自定义一个函数parse_user_info()，能传入一行上述数据，返回切分好的字段


然后可以通过如下sql完成需求：
create t_user
as
select
parse_user_info(info,0) as uid,
parse_user_info(info,1) as uname,
parse_user_info(info,2) as age,
parse_user_info(info,3) as birthday_date,
parse_user_info(info,4) as birthday_time,
parse_user_info(info,5) as address
from t_user_info;


实现关键：  自定义parse_user_info() 函数
实现步骤：
1、写一个java类实现函数所需要的功能
public class UserInfoParser extends UDF{
	// 1,zhangsan:18-1999063117:30:00-beijing
	public String evaluate(String line,int index) {
		String newLine = line.replaceAll(",", "\001").replaceAll(":", "\001").replaceAll("-", "\001");
		StringBuilder sb = new StringBuilder();
		String[] split = newLine.split("\001");
		StringBuilder append = sb.append(split[0])
		.append("\t")
		.append(split[1])
		.append("\t")
		.append(split[2])
		.append("\t")
		.append(split[3].substring(0, 8))
		.append("\t")
		.append(split[3].substring(8, 10)).append(split[4]).append(split[5])
		.append("\t")
		.append(split[6]);

		String res = append.toString();

		return res.split("\t")[index];
	}
}

2、将java类打成jar包: d:/up.jar
3、上传jar包到hive所在的机器上  /root/up.jar
4、在hive的提示符中添加jar包
hive>  add jar /root/up.jar;
5、创建一个hive的自定义函数名 跟  写好的jar包中的java类对应
hive>  create temporary function parse_user_info as 'com.doit.hive.udf.UserInfoParser';






















































