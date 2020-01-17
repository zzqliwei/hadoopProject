package com.westar.spark.rdd.keyvalue

import com.westar.spark.rdd.User
import org.apache.spark.{SparkConf, SparkContext}

/**
 * keyBy
 * groupBy
 */
object KeyValueCreationTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("KeyValueCreationTest")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val kvPairRDD =
      sc.parallelize(Seq(("key1", "value1"), ("key2", "value2"), ("key3", "value3")))
    kvPairRDD.collect()

    val personSeqRDD =
      sc.parallelize(Seq(User("jeffy", 30), User("kkk", 20), User("jeffy", 30), User("kkk", 30)))

    //将RDD变成二元组类型的RDD
    val keyByRDD = personSeqRDD.keyBy(x => x.userId)
    keyByRDD.collect()

    val keyRDD2 = personSeqRDD.map(user => (user.userId, user))

    val groupByRDD = personSeqRDD.groupBy(user => user.userId)
    groupByRDD.glom().collect()

    groupByRDD.foreach(println(_))

    val rdd1 = sc.parallelize(Seq("test", "hell"))
    rdd1.map(str => (str, 1))
    rdd1.collect()

  }

}
