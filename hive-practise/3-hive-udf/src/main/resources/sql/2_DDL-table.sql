
CREATE TABLE pokes(foo STRING,bar STRING) FROM FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/hadoop/hive-course/pokes.txt'

CREATE DATABASE IF NOT EXISTS pokes;

USE sensor;
--一：表的行格式（ROW FORMAT）
CREATE TABLE IF NOT EXISTS sensor_row_format(
id STRING,
event_id SRING,
event_type STRING,
part_name STRING,
part_number STRING,
verson STRING,
payload STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH 'hdfs://master:8020/usr/hadoop/hbase-course/20180506/omneo.csv' OVERWRITE INTO TABLE sensor_row_format;

--[ROW FORMAT]:表示表中的每一行的格式，可以取DELIMITED和SERDE两个值
--ROW FORMAT DELIMITED -->表示每一行是按照一定的分隔符分隔开的格式
--FIELDS TERMINATED BY ',' -->表示每一行的每一个字段是按照逗号分隔开的

-- ROW FORMAT SERDE ---> 自定义每一行的格式
--'org.apache.hadoop.hive.serde2.RegexSerDe'
--'org.apache.hive.hcatalog.data.JsonSerDe'
--'org.apache.hadoop.hive.serde2.OpenCSVSerDe'

-----------Regex-------------------
CREATE TABLE IF NOT EXISTS tomcat_access_log(
ip STRING,
userid STRING,
userName STRING,
time STRING,
url STRING,
status STRING,
file_size STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegrexSerDe'
WITH SERDEPEOPERTIES('input.regrex'='([^ ]*) ([^ ]*) ([^ ]*) (-|\\[.*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)');

LOAD DATA LOCAL INPATH '/home/hadoop/hbase-course/apache-tomcat-9.0.8/logs/localhost_access_log.2018-05-07.txt' OVERWRITE INTO TABLE tomcat_access_log;

--查询每天同一个ip访问了5次以上的日志
select substring(time,2,14) datetime, ip, count(*) as count
from tomcat_access_log
group by substring (time,2,14),ip
having count > 5
sort by datatime,count;

------------JSON-------------------
CREATE TABLE json_table(a STRING,b STRING) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';

LOAD DATA LOCAL INPATH '/home/hadoop/hive-course/json_table.json' OVERWRITE INTO TABLE json_table;

----------------CSV--------------------
CREATE TABLE mu_table(a STRING,b STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerDe'
WITH SERDEPROPERTIES(
"sepatatorChar"="\t",
"quoteChar"="'",
"escapeChar"="\\"
)

--(二)：表结构的查询
SHOW TABLES -->表示查看一个DB里面有多少张表
--下面功能是一样的
DESC tomcat_acess_log;
DESCRIBE tomcat_access_log;

DESCRIBE EXTENDED sensor_row_format;
DESCRIBE FORMATTED sensor_row_format;

SHOW CREATE TABLE sensor_row_format;
SHOW CREATE TABLE tomcat_access_log;

--(三)：Storage Format
--1、默认的存储格式是text format
CREATE TABLE IF NOT EXISTS sensor_format_text(
id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

SHOW CREATE TABLE sendsor_format_text;
--2、上面的语句和下面的语句是同等的
CREATE TABLE `sensor_format_text`(
  `id` string,
  `event_id` string,
  `event_type` string,
  `part_name` string,
  `part_number` string,
  `version` string,
  `payload` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://master:8020/user/hive/warehouse/sensor.db/sensor_format_text'
TBLPROPERTIES (
  'transient_lastDdlTime'='1526541651');

--3、Hive的查询处理过程
--SerDe 是 "Serializer and Deserializer." 的缩写
--Hive uses SerDe (and FileFormat) to read and write table rows.
--读的过程步骤：HDFS files --> InputFileFormat --> <key, value> --> Deserializer --> Row object
--写的过程步骤：Row object --> Serializer --> <key, value> --> OutputFileFormat --> HDFS files

--4、其他的File Format
--parquet
CREATE TABLE IF NOT EXISTS sensor_parquet(
id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
  SORED BY PARQUET;

  SHOW CREATE TABLE sensor_parquet;
CREATE TABLE IF NOT EXISTS sensor_parquet(
id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

LOAD DATA LOCAL INPATH '/user/hadoop/hive-dourse/parqyet' OVERWRITE INTO TABLE sensor_parquet;

--orc
CREATE TABLE IF NOT EXISTS sensor_orc(
  id STRING, 
  event_id STRING, 
  event_type STRING, 
  part_name STRING, 
  part_number STRING, 
  version STRING, 
  payload STRING) 
STORED AS ORC;

SHOW CREATE TABLE sensor_orc;

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'

LOAD DATA INPATH "/user/hadoop/hive-course/orc" OVERWRITE INTO TABLE sensor_orc;   

--avro
CREATE TABLE IF NOT EXISTS sensor_avro(
  id STRING, 
  event_id STRING, 
  event_type STRING, 
  part_name STRING, 
  part_number STRING, 
  version STRING, 
  payload STRING) 
STORED AS AVRO;

SHOW CREATE TABLE sensor_avro;

ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  
LOAD DATA INPATH "/user/hadoop/hive-course/avro" OVERWRITE INTO TABLE sensor_avro; 

--sequence File
CREATE TABLE IF NOT EXISTS sensor_sequence(
  id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS SEQUENCEFILE;

--需要自定义SERDE
SHOW CREATE TABLE sensor_sequence;

STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.SequenceFileInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.mapred.SequenceFileOutputFormat'

LOAD DATA INPATH "/user/hadoop/hive-course/sequence" OVERWRITE INTO TABLE sensor_sequence;

---- (四)：表的种类
--创建一张内部表(managed table)
CREATE TABLE IF NOT EXISTS sensor.sensor_manager(
id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
  ROW FORMAT DELIMITED FIELDS TERMINNATED BY ','
  STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/hadoop/hive-course/omneo.csv' OVERWRITE INTO TABLE sensor_managed;

TRUNCATE TABLE sensor_managed; //清除表的数据

LOAD DATA INPATH 'hdfs://master:8020/user/hadoop/hive-course/omneo.csv' OVERWRITE INTO TABLE sensor_managed;

DROP TABLE sensor_managed; //删除表的时候，数据也删除了

--创建一张外部表(external table)
CREATE EXTERNAL TABLE IF NOT EXISTS sensor_external(
 id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFIEL
LOCATION 'gdfs://master:8020/user/hadoop/hive-course/omneo';

DROP TABLE sensor_external; //删除表的时候，数据不会被删除

-- 创建一张临时表(temporary table), 只存在于当前会话的表，一旦会话关闭，则表被删除
CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS sensor(
 id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORE AS TEXTFILE
LOCATION 'hdfs://maser:8020/user/hadoop/hive-course/omneo';

--创建视图
CREATE VIEW [IF NOT EXISTS ]view_name [(column_name [COMMNET column_coment],...)] AS SELECT ....

CREATE VIEW IF NOT EXISTS alter_sensor_view AS select * from westar.sensor_external where event_type='ALTER';
DROP VIEW IF EXISTS alter_sensor_view;

--(五)：数据类型
# 1、numeric data types
TINYINT  1-byte 50Y
SMALLINT 2-byte 50S
INT		4-byte 50
BIGINT	8-byte 50L
FLOAT	4-byte
DOUBLE	8-byte
DECIMAL	17-byte

CREATE TABLE customer(id BIGINT, age TINYINT)
CREATE TABLE ORDER (id BIGINT, price DECIMAL(10, 2))

# 2、string data types
STRING：使用单引号或者双引号括起来的字符串
VARCHAR：长度可变的字符串
CHAR：定长字符串，如果存储的字符串的长度小于指定的长度，则用空格填充

CREATE TABLE customer(id BIGINT, name STRING, sex CHAR(6), role VARCHAR(64))


# 3、Date/Time data types
DATE表示日期，从0000-01-01到9999-12-31
TIMESTAMP表示时间，包含年月日时分秒

CREATE TABLE date_example(id INT, created_at DATE, updated_at TIMESTAMP)

# 4、boolean date types
BOOLEAN      true and false

# 5、complex data types
STRUCT 语法：STRUCT<col_name : data_type, ....>
MAP
ARRAY

CREATE TABLE employee(
name STRING,
salary FLOAT,
subordinnates ARRAY<string>,
deducations MAP<string,FLOAT>,
address STRUCT<stree:string,cit:string,state:string,zip:int>)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMANATED BY ':'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/hadoop/hive-course/employee.txt' OVERWRITE INTO TABLE employee;

select subordinnates[0] from empolyee;
select deducations['Federal Taxes'] from employee;
select size(deducations) FROM employee;
select address.state,address.stree from empoyee;
--(六)：分区表
-- 创建一张分区内部表：
CREATE TABLE IF NOT EXISTS sensor_managed_partition(
id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
PARTITIONED BY (year INT,month INT,day INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH 'hdfs://master:8020/user/hadoop/hive-course.omneo/20180508' OVERWRITE INTO TABLE sensor_managed_partition PARTITION(year=2018,month=201805,day=2010508);
LOAD DATA INPATH 'hdfs://master:8020/user/hadoop/hive-course/omneo/20180509' OVERWRITE INTO TABLE sensor_managed_partition PARTITION(year=2018, month=201805, day=20180509);

-- 创建一张分区外部表：
hadoop fs -mkdir -p /user/hadoop/hive-course/omneo/year=2018/month=201805/day=20180508
hadoop fs -put omneo.csv /user/hadoop/hive-course/omneo/year=2018/month=201805/day=20180508

hadoop fs -mkdir -p /user/hadoop/hive-course/omneo/year=2018/month=201805/day=20180509
hadoop fs -put omneo.csv /user/hadoop/hive-course/omneo/year=2018/month=201805/day=20180509
CREATE EXTERNAL TABLE IF NOT EXISTS sensor_external_partition (
  id STRING,
  event_id STRING,
  event_type STRING,
  part_name STRING,
  part_number STRING,
  version STRING,
  payload STRING)
PARTITIONED BY(year INT, month INT, day INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://master:8020/user/hadoop/hive-course/omneo';

ALTER TABLE sensor_external_partition ADD PARTITION (year=2018, month=201805, day=20180508) LOCATION '/user/hadoop/hive-course/omneo/year=2018/month=201805/day=20180508';
ALTER TABLE sensor_external_partition ADD PARTITION (year=2018, month=201805, day=20180509);

-- 查看一张表有多少的PARTITION
SHOW PARTITIONS sensor_external_partition;
-- rename partition
-- 外部表的话，不会改变数据文件的路径
ALTER TABLE sensor_external_partition PARTITION (year=2018, month=201805, day=20180509) RENAME TO PARTITION (year=2018, month=201806, day=20180609);
SHOW PARTITIONS sensor_external_partition;

-- 内部表的话，会改变数据文件的路径
ALTER TABLE sensor_managed_partition PARTITION (year=2018, month=201805, day=20180509) RENAME TO PARTITION (year=2018, month=201806, day=20180609);
SHOW PARTITIONS sensor_managed_partition;

-- exchange partition
-- 将sensor_external_partition分区复制给sensor_external_partition_like
-- sensor_external_partition的分区被删除了
CREATE TABLE sensor_external_partition_like LIKE sensor_external_partition;
ALTER TABLE sensor_external_partition_like EXCHANGE PARTITION(year=2018, month=201806, day=20180609) WITH TABLE sensor_external_partition;

-- recover partition
hadoop fs -mkdir -p /user/hadoop/hive-course/omneo/year=2018/month=201805/day=20180510
hadoop fs -put omneo.csv /user/hadoop/hive-course/omneo/year=2018/month=201805/day=20180510
-- 修复在对应HDFS上有文件而在metastore中没有对应分区的分区
MSCK REPAIR TABLE sensor_external_partition;

-- drop partition
ALTER TABLE table_name DROP [IF EXISTS] PARTITION partition_spec[, PARTITION partition_spec, ...] [PURGE];
-- 删除分区的时候外部表的数据不会被清除掉
ALTER TABLE sensor_external_partition DROP IF EXISTS PARTITION (year=2018, month=201805, day=20180510);
-- 删除分区的时候内部表的数据会被清除掉，放到了HDFS的Trash中
ALTER TABLE sensor_managed_partition DROP IF EXISTS PARTITION (year=2018, month=201805, day=20180508) PURGE;

[PURGE] : 内部表加上 PURGE 的话，则数据不会放到HDFS的Trash中，永远被删除了

-- 删除分区数据(只能删除内部表的分区数据)，数据虽然删除，但是表的分区还在
TRUNCATE TABLE table_name [PARTITION partition_spec];
TRUNCATE TABLE sensor_managed_partition PARTITION(year=2018, month=201806, day=20180609);
--(七)：表和分区的修改
--表的重命名：
ALTER TABLE sensor_managed_partition RENAME TO sensor_managed_partition_new;
ALTER TABLE sensor_managed_partition_new RENAME TO sensor_managed_partition;

-- 每一个分区对应的存储数据文件的格式可以不同
hadoop fs -mkdir /user/hadoop/hive-course/omneo-new
ALTER TABLE sensor_external_partition SET LOCATION "/user/hadoop/hive-course/omneo-new";
SELECT COUNT(*) FROM sensor_external_partition;
ALTER TABLE sensor_external_partition PARTITION (year=2018, month=201805, day=20180508) SET LOCATION "/user/hadoop/hive-course/omneo-new/year=2018/month=201805/day=20180508";

-- 但是20180509这一天的数据却是以PARQUET的文件格式存在
ALTER TABLE sensor_external_partition PARTITION (year=2018, month=201805, day=20180509)  SET FILEFORMAT PARQUET;
ALTER TABLE sensor_external_partition PARTITION (year=2018, month=201805, day=20180509) SET LOCATION "/user/hadoop/hive-course/omneo-new/year=2018/month=201805/day=20180509";
--修改一张表的file format，不要轻易使用
ALTER TABLE sensor_external_partition SET FILEFORMAT PARQUET;

--字段名的修改：
CREATE TABLE test_change (a int comment "this", b int, c int);
--将字段a修改为a1
ALTER TABLE test_change CHANGE a a1 INT;
--将字段a1重名为a2，且将字段a2的数据类型设置为STRING，并且将a2放置在字段b的后面
ALTER TABLE test_change CHANGE a1 a2 STRING AFTER b;
--给字段a2添加commnent
ALTER TABLE test_change CHANGE a2 a2 STRING COMMENT 'this is column a1';
--用于新增不存在的列
ALTER TABLE test_change ADD COLUMNS (a int, d int);