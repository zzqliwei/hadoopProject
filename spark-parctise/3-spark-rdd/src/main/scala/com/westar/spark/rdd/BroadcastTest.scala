package com.westar.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * broadcast
 */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("BroadcastTest")

    val sc = new SparkContext(conf)

    val lookupTable = Map("plane" -> "sky", "fish" -> "sea", "people" -> "earth")

    val lookupTableB = sc.broadcast(lookupTable)

    val logData = sc.parallelize(Seq("plane", "fish", "duck", "dirty", "people", "plane"), 2)

    logData.foreach(str =>{
      val replaceStrOpt = lookupTableB.value.get(str)
      if(replaceStrOpt.isDefined){
        println(s"============找到了[${str}]对应的值[${replaceStrOpt.get}]")
      }else{
        println(s"============未找到[${str}]对应的值")
      }
    })
  }

}
