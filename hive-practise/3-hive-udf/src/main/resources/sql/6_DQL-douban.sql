--在master上启动metastore:
-- nohup hive --service metastore > ~/bigdata/apache-hive-2.3.3-bin/logs/metastore.log 2>&1 &

--在slave1上启动hiveserver2服务
-- nohup $HIVE_HOME/bin/hiveserver2 > ~/bigdata/apache-hive-2.3.3-bin/logs/hiveserver2.log 2>&1 &

--使用beeline进行hive的查询

CREATE DATABASE IF NOT EXISTS douban;

USE douban;
--1、创建一张movie_links表
CREATE TABLE movie_links(
    movieid STRING,
    url STRING)
STORED AS TEXTFILE ;

--创建一张临时外部表，未来导入数据
CREATE EXTERNAL TABLE movie_links_tmp(
movieid STRING,
url STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/user/hadoop/hive-course/douban/links";

select substring(url, locate("/subject/", url, 1) + 9),url from movie_links_tmp limit 4;
select split(url, '/')[4], url from movie_links_tmp limit 4;
--将/user/hadoop-twq/hive-course/douban/links中的数据导入到movie_links表中
INSERT OVERWRITE TABLE movie_links select split(url, '/')[4], url from movie_links_tmp;
DROP TABLE movie_links_tmp;

--2、使用Spark Job将/user/hadoop-twq/hive-course/douban/movie.csv数据导入到hive的movie表中
/*
spark-submit --class com.westar.douban.MovieEtl \
--master spark://master-dev:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 2 \
--executor-cores 1 \
--conf spark.douban.movie.path=hdfs://master-dev:9999/user/hadoop-twq/hive-course/douban/movie.csv \
 /home/hadoop-twq/hive-course/hive-course-1.0-SNAPSHOT.jar douban movie
*/

--3、SELECT .... FROM ... 字句
SELECT COUNT(*) FROM movie;
SELECT * FROM movie LIMIT 2;

select movieid, moviename, directors from movie limit 10;
select movieid, moviename, directors[0] from movie limit 10;

select movieid, moviename, initialreleasedatemap from movie limit 10;
select movieid, moviename, initialreleasedatemap["美国"] from movie limit 10;

select m.movieid, m.moviename from movie m where year = 1939 limit 10;
select m.movieid, m.moviename from movie m where summary like '%朋友%' limit 10;
select m.movieid, m.moviename from movie m where moviename rlike '.*[\s+].*';

--查询评分是否和(评论数 / 视频时长)占比有关系
SELECT  m.movieid, m.moviename, commentnum / showtime as rate, commentscore
FROM movie m
order by rate desc limit 10;

select m.movieid, m.moviename, commentnum / showtime as rate, commentscore
from movie m
where (commentnum / showtime) > 5000
order by rate desc
limit 40;

--4、UDF(User Defined Function)，内置UDF --单行函数  输入是一行但是输出是一行
--4.1、数学函数round
select m.movieid, m.moviename, commentnum / showtime as rate, round(commentnum / showtime, 2) as round_rate, commentscore
from movie m
order by rate desc
limit 10;

--4.2、集合函数
--查询出属于"剧情"类型的电影
select m.movieid, m.moviename, category from movie m where array_contains(category, "剧情");
--查询出在中国大陆上映过的电影
select m.movieid, m.moviename, initialreleasedatemap from movie m where array_contains(map_keys(initialreleasedatemap), "中国大陆");
--4.3、类型转换函数
select cast(movieid as int) as int_movieid, moviename from movie limit 10;
--4.4、时间函数
--查询出在美国上映的时间的月份和日
select initialreleasedatemap["美国"], month(initialreleasedatemap["美国"]), day(initialreleasedatemap["美国"]) from movie limit 10;
--4.5、字符串函数
select format_number(commentscore, 4) from movie limit 10;
--字符串拼接
select concat(m.movieid, m.moviename, m.commentnum) from movie m limit 10;

--CASE WHEN语句
select commentscore,
       case
           when (commentscore > 8) then "优秀"
           when (commentscore > 5) then "中等"
           else "很差"
           end
from movie;

select commentscore,
       case  commentscore
           when 9.0 then "score is 9.0"
           when 8.0 then "score is 8.0"
           else "other"
           end
from movie;

--嵌套查询
from(
        select * from movie
        where array_contains(category, "剧情")
          and array_contains(category, "犯罪")
    ) e
select e.moviename where e.commentscore > 9;

--CTE查询
select a.moviename, b.url
from movie a join movie_links b on a.movieId = b.movieId
where array_contains(a.category, "剧情")
  and array_contains(a.category, "犯罪")
  and a.commentscore > 9
  and b.movieId > "300000";

with t1 as (
    select * from movie
    where array_contains(category, "剧情")
      and array_contains(category, "犯罪")
),
     t2 as (select * from movie_links where movieId > "300000")
select a.moviename, b.url from t1 a join t2 b on a.movieId = b.movieId where commentscore > 9;

--5、UDAF(User Defined Aggregation Function) --多行函数  输入是多行但是输出是一行
select avg(commentscore) from movie;
select max(commentscore) from movie;
select min(commentscore) from movie;
select sum(commentnum) from movie;

select year, sum(commentnum)
from movie
group by year
having sum(commentnum) > 1000000;

select year,count(*) from movie group by year;

select collect_list(year) from movie;
select collect_set(year) from movie;

--6、UDTF(User Defined Table Function) --输入是一行但是输出是多行
select m.movieid, m.moviename, category from movie m limit 10;
select explode(category) from movie m limit 10;
select posexplode(category) from movie m limit 10;

select explode(initialreleasedatemap) from movie m limit 10;

--通过Lateral view可以方便的将UDTF得到的行转列的结果集合在一起提供服务
--统计每一个类别有多少电影
select ctgy, count(*) from movie lateral view explode(category) category as ctgy group by ctgy;

--7、Join语句
select m.*, l.* from movie m join movie_links l on m.movieId = l.movieId limit 10;
select m.movieId, m.moviename, l.url from movie m join movie_links l on m.movieId = l.movieId limit 10;
select m.movieId, m.moviename, l.url from movie m, movie_links l where m.movieId = l.movieId limit 10;

--left/right/full join
select m.movieId, m.moviename, l.url from movie m left outer join movie_links l on m.movieId = l.movieId limit 10;
select m.movieId, m.moviename, l.url from movie m right outer join movie_links l on m.movieId = l.movieId limit 10;
select m.movieId, m.moviename, l.url from movie m full outer join movie_links l on m.movieId = l.movieId limit 10;

--left semi join  代替 IN 字句的
select m.* from movie m where m.movieId in (select l.movieId from movie_links l);  -- 性能非常差，使用left semi join代替
select m.* from movie m left semi join movie_links l on m.movieId = l.movieId limit 10;

--但是movie_links中的字段不能在where字句和select字句中出现，比如：下面的两个语句都会报错：
select m.*, l.* from movie m left semi join movie_links l on m.movieId = l.movieId limit 10;
select m.* from movie m left semi join movie_links l on m.movieId = l.movieId where l.url like "%234%" limit 10;

--map-side join - 将数据量比较小的表，直接缓存在内存，然后在map段完成join，性能提高不少
--第一种方式，比较老的方式
select /*+ MAPJOIN(movie_links)*/  m.*, l.* from movie m join movie_links l on m.movieId = l.movieId limit 10;
--第二种方式，设置参数的方式：
set hive.auto.convert.join=true;
select m.*, l.* from movie m join movie_links l on m.movieId = l.movieId limit 10;

set hive.auto.convert.join=false;
select m.*, l.* from movie m join movie_links l on m.movieId = l.movieId limit 10; -- 会产生reduce任务

--bucket map join, 当两张join的表都很大的时候使用这种方式
--两张表对join字段进行分bucket，且buckets的数量是倍数关系，然后使用bucket map join性能会很高
set hive.optimize.bucketmapjoin=true;
select /*+ MAPJOIN(movie_links)*/  m.*, l.* from movie m join movie_links l on m.movieId = l.movieId limit 10;

--bucket sort merge map join
--当数据在bucket中是按照顺序排列的，且两张表的相同的字段的buckets的数量是一样多的，则可以使用这种方式的join
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
select /*+ MAPJOIN(movie_links)*/  m.*, l.* from movie m join movie_links l on m.movieId = l.movieId limit 10;

--9、聚合高级函数
-- grouping sets 将多个分组聚合的结果放在一起
select year, ctgy, count(*) from movie lateral view explode(category) category as ctgy group by year, ctgy;

--查询出每年一共多少电影以及每一个种类一共多少电影
select year, null, count(*) from movie group by year
union all
select null, ctgy, count(*) from movie lateral view explode(category) category as ctgy group by ctgy;

select
    year, ctgy, count(*)
from movie lateral view explode(category) category as ctgy
group by year, ctgy
    grouping sets(year, ctgy);

select
    year, ctgy, count(*), grouping__id
from movie lateral view explode(category) category as ctgy
group by year, ctgy
    grouping sets(year, ctgy)
order by grouping__id;

select year, null, count(*), 1 as grouping_id from movie group by year
union all
select null, ctgy, count(*), 2 as grouping_id from movie lateral view explode(category) category as ctgy group by ctgy;

--查询出每年一共多少电影、每一个种类一共多少电影以及每一年的每一个种类一共多少电影
select
    year, ctgy, count(*), grouping__id
from movie lateral view explode(category) category as ctgy
group by year, ctgy
    grouping sets(year, ctgy, (year, ctgy))
order by grouping__id;

select year, null, count(*), 1 as grouping_id from movie group by year
union all
select null, ctgy, count(*), 2 as grouping_id from movie lateral view explode(category) category as ctgy group by ctgy
union all
select year, ctgy, count(*), 3 as grouping_id from movie lateral view explode(category) category as ctgy group by year, ctgy;

--cube
--查询出每年一共多少电影、每一个种类一共多少电影以及每一年的每一个种类一共多少电影
select
    year, ctgy, count(*), grouping__id
from movie lateral view explode(category) category as ctgy
group by year, ctgy
with cube
order by grouping__id desc;

select year, ctgy, count(*), 0 as grouping_id from movie lateral view explode(category) category as ctgy group by year, ctgy
union all
select year, null, count(*), 1 as grouping_id from movie group by year
union all
select null, ctgy, count(*), 2 as grouping_id from movie lateral view explode(category) category as ctgy group by ctgy
union all
select null, null, count(*), 3 as grouping_id from movie

--rollup
--查询出每年个种类总共多少部电影、每年多少部电影以及总共多少部电影
select
    year, ctgy, count(*), grouping__id
from movie lateral view explode(category) category as ctgy
group by year, ctgy
with rollup
order by grouping__id;

--数据可视化：1、一次性查询结果；2、支持不同条件的查询结果
--1、一次性查询结果
--获取所有的电影类别种类
--这里查询结果是以类型中所有的元素作为判别标准
--第二行为指定行分隔符，并使用explode关键字，以类型array中的每一个元素作为关键字进行统计
insert overwrite local directory "/home/hadoop-twq/hive-course/douban"
    row format delimited fields terminated by "\t"
select cat, count(*) from movie lateral view explode(category) category as cat group by cat;

--2、支持不同条件的查询结果
SELECT /*+ MAPJOIN(movie_links)*/, m.movieId, m.moviename, l.url from movie m join movie_links l on m.movieId = l.movieId where year=2018 order by m.commentscore desc limit 10;




