package com.westar.spark.rdd.scala

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tangweiqun on 2017/8/28.
  */
object SeqTest {

  def main(args: Array[String]): Unit = {
    val seqStr: Seq[String] = Seq.apply("test", "my")
    val seq: Seq[Int] = Seq(4, 3, 2, 3, 9)

    seq.length
    seq.size

    seq.map(addTestStr)
    seq.map(addOne)

    seq.map((x: Int) => x + 1)
    seq.map(x => x + 1)
    seq.map(_ + 1)

    seq.flatMap(oneToSeq)

    seq.filter(x => x > 3)
    seq.filterNot(x => x > 3)

    seq.reduce((x, y) => x + y)

    seq.fold(0)((x, y) => x + y)

    seq.filter(_ > 3).map(_ + "test")
    seq.foldLeft(ArrayBuffer.newBuilder[String])((arr, x) => {
      if (x > 3) {
        arr += (x + "test")
      } else {
        arr
      }
    })

    seq.foreach(x => println(x))

    seq.sorted

    seq.zipWithIndex
    seqStr.zip(seq)

  }

  def addOne(x: Int): Int = {
    x + 1
  }

  def addTestStr(x: Int): String = {
    x + "test"
  }

  def oneToSeq(x: Int): Seq[Int] = {
    0.to(x)
  }

}
