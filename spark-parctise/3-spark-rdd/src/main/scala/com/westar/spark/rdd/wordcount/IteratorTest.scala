package com.westar.spark.rdd.wordcount

import scala.io.Source

object IteratorTest {
  def main(args: Array[String]): Unit = {
    val line = Source.fromFile("test.txt").getLines()
    println(line.next())
  }

}
