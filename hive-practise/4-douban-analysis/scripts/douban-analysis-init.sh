#!/usr/bin/env bash

hadoop fs -rm -r /user/hadoop/hive-course/douban
hadoop fs -mkdir -p /user/hadoop/hive-course/douban
hadoop fs -mkdir -p /user/hadoop/hive-course/douban/links

hadoop fs -chmod -R 777 /user/hadoop/hive-course/douban

$HIVE_HOME/bin/beeline -u jdbc:hive2://slave1:10000 -n hadoop -f ./sql/douban_init.sql 
