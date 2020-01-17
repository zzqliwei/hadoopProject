package com.westar.spark.rdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * persist
 * getStorageLevel
 * cache
 * count
 * unpersist
 */
object RDDPersistApiTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")
    val sc = new SparkContext(conf)

    val hdfsFileRDD = sc.textFile("hdfs://hadoop0:9000/users/hadoop-twq/person.json")
    val mapRDD = hdfsFileRDD.flatMap(str => str.split(" "))

    //存储级别：
    //MEMORY_ONLY: 只存在内存中
    //DISK_ONLY: 只存在磁盘中
    //MEMORY_AND_DISK: 先存在内存中，内存不够的话则存在磁盘中
    //OFF_HEAP: 存在堆外内存中
    mapRDD.persist(StorageLevel.MEMORY_ONLY)
    mapRDD.getStorageLevel

    mapRDD.getStorageLevel // None
    mapRDD.cache() //表示只存在内存中

    mapRDD.count()

    mapRDD.collect()

    mapRDD.unpersist()

    hdfsFileRDD.unpersist()
  }

}
