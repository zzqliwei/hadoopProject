#!/usr/bin/env bash

# use crontab schedule this script, for example:
# crontab -e
# 0 3 * * * /home/hadoop/hive-course/douban/scripts/douban-analysis-etl.sh /home/hadoop/hive-course/douban > /home/hadoop/hive-course/douban/logs/`date -d last-day +\%Y\%m\%d`.log 2>&1


douban_base_path=$1
 
source /home/hadoop/.bash_profile

# 1 movie links etl
$HIVE_HOME/bin/beeline -u jdbc:hive2://slave1-dev:10000 -n hadoop -f ${douban_base_path}/scripts/sql/movie_links_etl.sql

# 2 movie etl
export HADOOP_CONF_DIR=/home/hadoop/bigdata/hadoop-2.7.5/etc/hadoop
spark-submit --class com.twq.MovieEtl \
--master yarn \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 2 \
--executor-cores 1 \
--conf spark.douban.movie.path=hdfs://master:9999/user/hadoop/hive-course/douban/movie-video.csv \
${douban_base_path}/jars/etl-1.0-SNAPSHOT.jar douban movie
