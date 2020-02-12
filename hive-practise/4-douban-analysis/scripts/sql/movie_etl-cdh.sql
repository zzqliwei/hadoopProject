
USE douban;

--创建一张临时外部表，为了导数据用
CREATE EXTERNAL TABLE `movie_tmp`(
       movieId string,
       movieName string,
       year int,
       directors array<string>,
       scriptsWriters array<string>,
       stars array<string>,
       category array<string>,
       nations array<string>,
       language array<string>,
       showtime int,
       initialReleaseDateMap map<string, string>,
       commentNum int,
       commentScore float,
       summary string)
STORED AS PARQUET
LOCATION "/user/hadoop/hive-course/douban/parquet";

--将/user/hadoop/hive-course/douban/links中的数据导入到movie_links表中
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE movie PARTITION(year) select
  movieId, movieName, directors,
  scriptsWriters, stars, category, nations,
  language, showtime, initialReleaseDateMap,
  commentNum, commentScore, summary, year
from movie_tmp;

DROP TABLE movie_tmp;

!exit