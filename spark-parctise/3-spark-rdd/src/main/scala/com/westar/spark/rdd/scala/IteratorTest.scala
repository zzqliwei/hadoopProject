package com.westar.spark.rdd.scala

/**
  * Created by tangweiqun on 2017/8/28.
  */
object IteratorTest {
  def main(args: Array[String]): Unit = {
    val it = Iterator("is", "it", "silly", "?")

    while (it.hasNext) {
      println(it.next())
    }

    it.map(_ + "test")
    it.flatMap(_.toCharArray).foreach(println)
    it.filter(_.contains("y")).foreach(println)
    it.foreach(println)
    it.zipWithIndex.foreach(println)
    it.zip(Iterator(1, 2, 4)).foreach(println)

  }
}
