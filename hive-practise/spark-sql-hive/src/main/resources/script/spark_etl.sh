#!/usr/bin/env bash
spark-submit \
--class com.westar.spark.rdd.example.ClickTrackerEtl \
--master spark://master:7077 \
--deploy-mode client \
--driver-momory 1g \
--executor-memory 1g \
--num-executors 2 \
--jar parquet-avro-1.0.1.jar \
--conf spark.session.groupBy.numPartitions=2 \
--conf spark.tracter.trackerDataPath=hdfs://master:8020/user/hadoop/example/ \
spark-rdd-1.0-SNAPSHOT.jar \
nonLocal
