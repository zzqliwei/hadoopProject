--一：创建DataBase的语法：
CREATE (DATABASE|SCHEMA) [IF NOT EXISTS ] database_name
[COMMENT databse_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES (property_name=property_value,...)];
--说明：
--DATABASE|SCHEMA : 这两个是同一个东西，都表示数据库
--[IF NOE EXISTS] : 可选的，如果不是用这个，然后去创建一张已经存在的数据库的话则会报错
--[COMMENT] : 可选的，表示给创建的DB备注，必须备注在单引号中
--[LOCAITION] ： 可选的，DB的存储路径是我们在hive-site.xml中配置的hive.metastore.warehouse.dir(即默认为/usr/hive/warehouse)
-- 如果想给DB自定义存储路劲的话，则设置这个选项
--[WITH DBPROPERTIES] : 可选的，表示给创建的DB的属性

--例子
CREATE DATABASE hive_learning;
CREATE DATABASE IF NOT EXISTS hive_learning;
CREATE DATABASE IF NOT EXISTS hive_test
    COMMENT 'just a test db'
    LOCATION 'hdfs://master:8020/usr/hadoop/hive-course/dbs'
    WITH DBPROPERTIES ('Create by' = 'westar','Create on'='2020-02-11')

--二、show database
SHOW DATABASES [LIKE identifier_with_wildcards];

SHOW DATABSES;
SHOW DATABASES LIKE 'hive*';

--三、describe DATABASE
DESCRIBE DATABASE [EXTENDED] database_name;

--[EXETENDED] : 表示将DB的属性也展示出来

DESCRIBE DATABASE hive_test;
DESCRIBE DATABASE EXTENDED hive_test;

--四 ： ALTER DATABASE
ALTER (DATABSE|SCHEMA) database_name SET DBPROPERTIES(porperty_name=peoperties_value,...);


ALTER DATABASE hive_test SET DBPROPERTIES('Create by' = 'jeffy')

DESCRIBE DATABASE EXTENDED hive_test;

--五、USE DATABASE
USE (DATABASE|SCHEMA) database_name;

USE DATABASE hive_test;

--六： DROP DATABASE
DROP (DATABASE|SCHEMA) [IF EXISTS] database_name [RESTRICT|CASCADE]
--[RESTRICT|CASCADE]:如果DB中还有表存在的话，那么在RESTRICT模式下，则不能删除DB
--如果是CASCADE模式下，回首先删除这个DB下的所有表，然后再删除这个DB
--hive 默认是RESTRICT模式

