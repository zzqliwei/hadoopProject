package com.westar.spark.rdd

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}

/**
 * map
 * flatmap
 * mappartitionts
 */
object MapApiTest {
  def getInitNumber(source: String): Int = {
    println(s"get init number from ${source}, may be take much time........")
    TimeUnit.SECONDS.sleep(2)
    1
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val listRDD = sc.parallelize[Int](Seq(1, 2, 3, 3), 2)

    val mapRDD = listRDD.map(x => x + 1)
    mapRDD.collect()

    val users = listRDD.map { x =>
      if (x < 3) User("小于3", 22) else User("大于3", 33)
    }
    users.collect()

    val l  = List(List(1,2,3), List(2,3,4))
    l.map(x => x.toString())
    l.flatMap(x => x)

    val flatMapRDD = listRDD.flatMap(x => x.to(3))
    flatMapRDD.collect()

    val filterRDD = listRDD.filter(x => x != 1)
    filterRDD.collect()

    //将rdd的每一个分区的数据转成一个数组，进而将所有的分区数据转成一个二维数组
    val glomRDD = listRDD.glom()
    glomRDD.collect() //Array(Array(1, 2), Array(3, 3))

    val mapPartitionTestRDD = listRDD.mapPartitions (iterator => {
      iterator.map(x => x + 1)
    })
    mapPartitionTestRDD.collect()

    val mapWithInitNumber = listRDD.map(x => {
      val initNumber = getInitNumber("map")
      x + initNumber
    })
    mapWithInitNumber.collect()

    val mapPartitionRDD = listRDD.mapPartitions (iterator => {
      //和map api的功能是一样，只不过map是将函数应用到每一条记录，而这个是将函数应用到每一个partition
      //如果有一个比较耗时的操作，只需要每一分区执行一次这个操作就行，则用这个函数
      //这个耗时的操作可以是连接数据库等操作，不需要计算每一条时候去连接数据库，一个分区只需连接一次就行
      val initNumber = getInitNumber("mapPartitions")
      iterator.map(x => x + initNumber)
    })
    mapPartitionRDD.collect()

    val mapPartitionWithIndexRDD = listRDD.mapPartitionsWithIndex((index, iterator) => {
      iterator.map(x => x + index)
    })
    mapPartitionWithIndexRDD.collect()
  }

}

case class User(userId: String, amount: Int)
