
USE douban;

--创建一张临时外部表，为了导数据用
DROP TABLE IF EXISTS movie_links_tmp;
CREATE EXTERNAL TABLE `movie_links_tmp`(
   `url` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/hadoop/hive-course/douban/links";

--将/user/hadoop/hive-course/douban/links中的数据导入到movie_links表中
INSERT OVERWRITE TABLE movie_links select split(url, '/')[4] as movieid, url from movie_links_tmp;

DROP TABLE movie_links_tmp;

!exit