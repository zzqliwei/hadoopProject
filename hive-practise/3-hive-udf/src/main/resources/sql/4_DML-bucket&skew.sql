USE dml;
--(一)： bucket
--下面的查询虽然在日期的过滤上查询非常快，但是一个分区的数据量比较到，那么过滤id的话就会比较慢
SELECT * FROM dml.sensor_partition WHERE day=20180508 and id='f277';

--我们可以通过bucket来提升上面的sql性能
CREATE TABLE sensor_partition_bucket(
id STRING,
event_id STRING,
event_type STRING,
part_name STRING,
part_number STRING,
version STRING,
payload STRING)
PARTITIONED BY (year INT,month INT,day INT)
CLUSTERED BY(id) INTO 12 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

SELECT * FROM dml.sensor_partition_bucket WHERE day=20180508 and id='f277';

SET hive.enforce.bucketing=true;--Hive 2.x 后就不需要了

INSERT OVERWRITE TABLE dml.sensor_partition_bucket PARTITION(year=2018, month=201805, day=20180508) SELECT * FROM dml.sensor_temp;
--因为"f277"。hashCode()%12 所以下面的语句只要去/user/hive/warehouse/dml.db/sensor_partition_bucket/year=2018/month=201805/day=20180508/000010_0这个文件中扫描就行
SELECT * FROM dml.sensor_partition_bucket WHERE day = 20180508 AND id = 'f277';
--加上SORTED BY(event_time)就是为了使得在一个bucket中的数据是按照event_time升序排列
CREATE TABLE sensor_partition_bucket(
id STRING,
event_id STRING,
event_type STRING,
part_name STRING,
part_number STRING,
version STRING,
payload STRING,
event_time STRING)
PARTITIONED BY (year INT,month INT,day INT)
CLUSTERED BY(id) SORTED BY(event_time) INTO 12 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

--bucket table的两个作用
1. 数据sampling
2. 提升某些查询操作效率，例如mapside join
--数据的取样
--tablesample是抽样语句，语法：TABLESAMPLE(BUCKET x OUT OF y ON bucket column)
--y必须是table总bucket数的倍数或是因子。hive根据y的大小，决定抽样的比例。
--例如 table 总共分了12份，
-- 当y=6的时候，抽取（12/6）2个bucket的数据
-- 当y=24的时候，抽取（12/24）0.5个bucket的数据

--x表示从哪个bucket开始抽取，例如table 总 bucket数为12，
-- tablesample(bucket 4 out of 6)，表示表示总共抽取（12/6=）2个bucket的数据，
-- 分别为第4个bucket和第（4+6=）10个bucket的数据。

SELECT * FROM sensor_partition_bucket TABLESAMPLE(BUCKET 4 OUT OF 6 ON id)

--(二)：Skewed Table
--有这么一种场景：一张表中的一列或者某几列的某些值占比特别的大
CREATE TABLE skewed_single(key STRING,value STRING)
SKEWED BY (key) on("1","5","6")
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE ;

CREATE EXTERNAL TABLE skewed_test_temp(key STRING,value STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/hive-course/skewed';

--Skewed Table：含有倾斜的信息的表
INSERT OVERWRITE TABLE skewed_single SELECT * FROM skewed_test_temp;
INSERT OVERWRITE TABLE dml.testskew SELECT * FROM dml.skewed_test_temp;

--List Bucketing Table是Skewed Table，此外，它告诉hive使用列表桶的特点：为倾斜值创建子目录。
CREATE TABLE list_bucket(key STRING,value STRING)
SKEWED BY (key) ON ("1","5","6") STORED AS DIRECTORIES
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE ;

INSERT OVERWRITE TABLE dml.list_bucket SELECT * FROM dml.skewed_test_temp;

--需要设置下面的属性才可以查询
set hive.mapred.supports.subdirectories=true;
set mapred.input.dir.recursive=true;
SELECT * FROM dml.list_bucket WHERE key=1;
--只会扫描/user/hive/warehouse/dml.db/list_bucket/key=1这个文件目录
--语法：
CREATE TABLE table_name (key STRING, value STRING)
SKEWED BY (key) ON (1,5,6) [STORED AS DIRECTORIES];

--(三)：总结，创建表的语句
CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name    -- (Note: TEMPORARY available in Hive 0.14.0 and later)
  [(col_name data_type [COMMENT col_comment], ... [constraint_specification])]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
  [CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
  [SKEWED BY (col_name, col_name, ...)                  -- (Note: Available in Hive 0.10.0 and later)]
     ON ((col_value, col_value, ...), (col_value, col_value, ...), ...)
     [STORED AS DIRECTORIES]
  [
   [ROW FORMAT row_format]
   [STORED AS file_format]
     | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)]  -- (Note: Available in Hive 0.6.0 and later)
  ]
  [LOCATION hdfs_path]
  [TBLPROPERTIES (property_name=property_value, ...)]   -- (Note: Available in Hive 0.6.0 and later)
  [AS select_statement];   -- (Note: Available in Hive 0.5.0 and later; not supported for external tables)

CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
  LIKE existing_table_or_view_name
  [LOCATION hdfs_path];

--(四)：Hive 和 HBase交互
--启动Hbase
-- hive --auxpath $HIVE_HOME/lib/zookeeper-3.4.6.jar,$HIVE_HOME/lib/hive-hbase-handler-2.3.3.jar,$HIVE_HOME/lib/hbase-server-1.1.1.jar --hiveconf hbase.zookeeper.quorum=master,slave1,slave2
CREATE TABLE pokes(foo INT,bar STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE ;

LOAD DATA LOCAL INPATH '/home/hadoop/hive-course/kv3.txt' OVERWRITE INTO TABLE pokes;

CREATE TABLE hase_table_1(key int,value STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val")
TBLPROPERTIES ("hbase.table.name" = "xyz", "hbase.mapred.output.outputtable" = "xyz");

INSERT OVERWRITE TABLE hase_table_1 SELECT * FROM pokes;

SELECT * FROM hase_table_1;
--SCAN 'xyz';

--Column Mapping
--多个column和多个column family的映射
CREATE TABLE hbase_table_2 (key int, value1 string, value2 int, value3 int)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
'hbase.column.mapping'=":key,a:b,a:c,d:e"
);
INSERT OVERWRITE TABLE hbase_table_2 SELECT foo, bar, foo+1, foo+2 FROM pokes;

--Hive中的Map对应HBase中的Column Family
CREATE TABLE hbase_table_map(value map<string,int>, row_key int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:,:key"
);

INSERT OVERWRITE TABLE hbase_table_map SELECT map(bar,foo),foo from pokes;

--从已经存在的HBASE表sensor中创建一张hive表，且表的
CREATE EXTERNAL TABLE sensor_hbase_avro
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,v:event_id",
"v.event_id.serialization.type" = "avro",
"v.event_id.avro.schema.url" = "hdfs://master:8020/user/hadoop/hive-course/sensor.avsc")
TBLPROPERTIES ( "hbase.table.name" = "sensor",
"hbase.mapred.output.outputtable" = "sensor",
"hbase.struct.autogenerate"="true",

"avro.schema.literal"='{
"type": "record",
"name": "Event",
"fields": [{ "name":"test_col", "type":"string"}]
}'
);

--上面的语句会报错，是hive的一个bug
--https://issues.apache.org/jira/browse/HIVE-17829?src=confmacro


