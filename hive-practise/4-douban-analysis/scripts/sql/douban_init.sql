CREATE DATABASE IF NOT EXISTS douban;

USE douban;

--1、创建一张movie_links表
DROP TABLE IF EXISTS movie_links;
CREATE TABLE `movie_links`(
   `movieid` string,
   `url` string)
STORED AS TEXTFILE;

--2、创建一张movie表
DROP TABLE IF EXISTS movie;
CREATE TABLE movie (
    movieId string,
    movieName string,
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
    summary string
)
PARTITIONED BY (year int)
STORED AS PARQUET;

!exit