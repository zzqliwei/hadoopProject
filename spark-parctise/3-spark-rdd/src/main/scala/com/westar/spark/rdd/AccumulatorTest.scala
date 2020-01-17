package com.westar.spark.rdd

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 计数器 CustomAccumulator
 */
object AccumulatorTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("AccumulatorTest")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)


    val longAccumulator = sc.longAccumulator("count mapped data")
    val collectionAccumulator = sc.collectionAccumulator[String]("collect mapped data")

    val mapAccumulator = new CustomAccumulator
    sc.register(mapAccumulator)

    val logData = sc.parallelize(Seq("plane", "fish", "duck", "dirty", "people", "plane"), 2)

    logData.foreach(str =>{
      if(str.equals("plane")){
        longAccumulator.add(1L)
      }
      try{
        // some code
      }catch{
        case e:Exception =>{
          collectionAccumulator.add(e.getMessage)
        }
      }

      mapAccumulator.add(str)
    })

    println(longAccumulator.sum)
    println(collectionAccumulator.value)
    println(mapAccumulator.value)
  }

}

class CustomAccumulator extends AccumulatorV2[String, ConcurrentHashMap[String, Int]] {

  private val map = new ConcurrentHashMap[String,Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, ConcurrentHashMap[String, Int]] = {
    val newAcc = new CustomAccumulator()
    newAcc.map.putAll(map)
    newAcc
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map.synchronized{
      if(map.containsKey(v)){
        map.put(v,map.get(v) + 1)
      }else{
        map.put(v,1)
      }
    }
  }

  override def merge(other: AccumulatorV2[String, ConcurrentHashMap[String, Int]]): Unit = other match {
    case o:CustomAccumulator =>{
      o.map.forEach(new BiConsumer[String,Int]{
        override def accept(key: String, value: Int): Unit = {
          if(map.containsKey(key)){
            map.put(key,map.get(key) + value)
          }else{
            map.put(key,value)
          }
        }
      })
    }
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: ConcurrentHashMap[String, Int] = {
   map
  }
}
