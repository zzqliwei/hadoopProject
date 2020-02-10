CREATE DATABASE IF NOT EXISTS westar;

use westar;
-根据我们在spark-rdd模块中定义的avro文件中的schema创建表，这个时候hive的metastore状态会变
-mysql中的hive数据库的表`TBLS`会新增一条记录
-且hive的数据warehouse中会新增一个文件目录：/user/hive/warehouse/twq.db/tracker_session
CREATE TABLE IF NOT EXISTS westar.tracker_session(
session_id string,
session_server_time string,
cookie string,
cookie_lable string,
ip string,
landing_url string,
pageview_count int,
click_count int,
domain string,
domain_lable string)
STORED AS PARQUET;

-将数据load到表tracker_sesion中
-/user/hadoop/example/trackerSession下的数据会被move到/user/hive/warehouse/tracker_session中

LOAD DATA INPATH 'hdfs://master:9999/user/hadoop/example/trackerSession' OVERWRITE INTO TABLE westar.tracker_session;

-查询表tracker_session
-根据hive的配置，先去metastore中查询先骨干的表的元数据信息
select count(*) from westar.tracker_session;
select * from westar.tracker_session;
select cookie,count(*) from westar.tracker_session group by cookie;

drop table tracker_session;
drop database westar;

