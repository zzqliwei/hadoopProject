CREATE DATABASE dml;
use dml;
--(一)：Loading files into tables
CREATE TABLE employee LIKE sensor.employee;

LOAD DATA LOCAL INPATH '/home/hadoop/hive-course/empoyee.txt' OVERWITE INTO TABLE employee;
LOAD DATA LOCAL INPATH '/home/hadoop/hive-course/empoyee.txt' INTO TABLE employee;

CREATE TABLE sensor_format LIKE sensor.sensor_format;
--1、path可以是一个文件目录
--2、原始的文件将move到表所在的LOCATION中
LOAD DATA INPATH 'hdfs://master:8020/user/hadoop/hive-course/omneo' OVERWRITE INTO TABLE sensor_format;
LOAD DATA INPATH 'hdfs://master:8020/user/hadoop/hive-course/omneo' INTO TABLE sensor_format;

CREATE TABLE sensor_managed_partition LIKE sensor.sensor_managed_partition;
LOAD DATA INPATH 'hdfs://master:9999/user/hadoop-twq/hive-course/omneo' OVERWRITE INTO TABLE sensor_managed_partition PARTITION(year=2018, month=201805, day=20180508);
LOAD DATA INPATH 'hdfs://master:9999/user/hadoop-twq/hive-course/omneo' INTO TABLE sensor_managed_partition PARTITION(year=2018, month=201805, day=20180508);


--LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2, ...)]

--(二)：INSERT data into Hive tables from queries
-- LOAD files into table的方法会删除了原始文件，如果不想删除原始文件的话，则可以通过INSERT data的方式导数据
--创建一张外部表，但是表的存储位置是在hive默认的位置，存储格式是PARQUET
CREATE EXTERNAL TABLE IF NOT EXISTS dml.sensor(
    id STRING,
    event_id STRING,
    event_type STRING,
    part_name STRING,
    part_number STRING,
    version STRING,
    payload STRING)
STORED AS PARQUET;

--创建一张临时的外部表
CREATE EXTERNAL TABLE IF NOT EXISTS sensor_temp(
id STRING,
event_id STRING,
event_type STRING,
part_name STRING,
part_number STRING,
version STRING,
payload STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://master:8020/user/hadoop/hive-course/omneo-temp/';

--将sensor_tmp中的数据导入到sensor中,如果sensor中有数据的话则删除，使用sensor_tmp数据覆盖掉原来的数据
INSERT OVERWRITE TABLE sensor select * from sensor_temp;
--将semsor_temp中数据追加到sensor中
INSERT INTO TABLE sensor select * from sensor_temp;

--语法：
--INSERT (OVERWRITE|INTO) TABLE table_name [PARTITION (partcol1=val1,partcol2=val2,...) [IF NOT EXISTS]] SELECT select_statement FROM form_statemant

--创建一个表分区
CREATE EXTERNAL TABLE IF NOT EXISTS sensor_partition(
id STRING,
event_id STRING,
event_type STRING,
part_name STRING,
part_number STRING,
version STRING,
payload STRING)
PARTITIONED BY (year INT,month INT,day INT)
STORED AS PARQUET ;
--将sensor_tmp中的所有数据导入到sensor_partition中的数据导入到sensor分区year=2018, month=201805, day=20180508中
INSERT OVERWRITE TABLE sensor_partition PARTITION (year=2018, month=201805, day=20180508) SELECT * FROM sensor_temp;
--将sensor_tmp中的数据追加到sensor_partition表的某个分区中
INSERT INTO TABLE sensor_partition PARTITION (year=2018, month=201805, day=20180508) SELECT * FROM sensor_temp;

--创建一张用于存放对传感器id、事件类型、零部件名称分组求和数据的表
CREATE EXTERNAL TABLE IF NOT EXISTS sensor_group_20180508(
id STRING,
event_type STRING,
part_name STRING,
cnt INT)
STORED AS AVRO;

INSERT OVERWRITE TABLE sensor_group_20180508
SELECT id, event_type, part_name, count(*) AS cnt
FROM sensor_partition WHERE day = 20180508
GROUP BY id, event_type, part_name;

--相当于上面的CREATE + INSERT, 但是只能创建内部表
CREATE TABLE IF NOT EXISTS sensor_group_20180508_a
STORED AS AVRO
AS SELECT id, event_type, part_name, count(*) AS cnt
FROM sensor_partition WHERE day = 20180508
GROUP BY id, event_type, part_name;

-- 使用多个 INSERT 字句
CREATE EXTERNAL TABLE IF NOT EXISTS sensor_alert LIKE sensor_partition;

FROM sensor_temp
INSERT OVERWRITE TABLE sensor_partition PARTITION (year=2018, month=201805, day=20180508) SELECT id, event_id, event_type, part_name, part_number, version, payload
INSERT OVERWRITE TABLE sensor_alert PARTITION (year=2018, month=201805, day=20180508) SELECT id, event_id, event_type, part_name, part_number, version, payload WHERE event_type='ALERT';
--语法:
--FROM from_statement
--INSERT OVERWRITE TABLE tablename1 [PARTITION (partcol1=val1, partcol2=val2, ...) [IF NOT EXISTS]] SELECT select_statement1
--    [INSERT OVERWRITE TABLE tablename2 [PARTITION (partcol1=val1, partcol2=val2, ...) [IF NOT EXISTS]] SELECT select_statement2]
--   [INSERT INTO TABLE tablename2 [PARTITION (partcol1=val1, partcol2=val2, ...)] SELECT select_statement2]
--   ...;

--(三) 动态分区
--静态分区
INSERT OVERWRITE TABLE sensor_partition PARTITION(year=2018, month=201805, day=20180509) SELECT * from sensor_temp;

CREATE EXTERNAL TABLE IF NOT EXISTS sensor_event_type_partition(
id STRING,
event_id STRING,
part_name STRING,
part_number STRING,
version STRING,
payload STRING)
PARTITIONED BY (year INT,month INT,event_type STRING)
STORED AS PARQUET ;

INSERT OVERWRITE TABLE sensor_event_type_partition PARTITION (year=2018,month,event_type)
select id,event_id,part_name,part_number,version,payload, month, event_type FROM sensor_partition;

--(四) Writing data into the filesystem from queries
--数据量大的话则写到HDFS的文件中
INSERT OVERWRITE DIRECTORY '/user/hadoop/hive-course/employee'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
SELECT * FROM dml.employee;

--数据量比较小的话则写到本地文件中
INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/hive-course/employee'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    COLLECTION ITEMS TERMINATED BY ','
    MAP KEYS TERMINATED BY ':'
    STORED AS TEXTFILE
SELECT * FROM dml.employee;

INSERT OVERWRITE DIRECTORY '/user/hadoop-twq/hive-course/employee-parquet'
    STORED AS PARQUET
SELECT * FROM dml.employee;

--(五)：Inserting values into tables from SQL
INSERT INTO TABLE sensor_partition PARTITION (year=2018, month=201805, day=20180508) VALUES("testid", "testEventId", "event_type", "part_name", "part_number", "version", "payload");











