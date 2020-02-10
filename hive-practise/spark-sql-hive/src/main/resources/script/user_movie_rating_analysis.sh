#!/usr/bin/env bash
spark-submit --class com.westar.sql.hive.example.UserMovieRationgEtl \
--master spark://master:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 2 \
--executor-cores 1 \
--conf spark.user.movie.rating.basePath=hdfs://master:8020/user/hadoop/ml-100 \
/home/hadoop/spark-course/spark-sql-hive-1.0-SNAPSHOT.jar

spark-submit --class com.westar.sql.hive.example.ALSExample \
--master spark://master:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 2\
--executor-cores 1 \
--jars /home/hadoop/spark-course/spark-dataset/mysql-connector-java-5.1.44-bin.jar \
    /home/hadoop/spark-course/spark-sql-hive-1.0-SNAPSHOT.jar
