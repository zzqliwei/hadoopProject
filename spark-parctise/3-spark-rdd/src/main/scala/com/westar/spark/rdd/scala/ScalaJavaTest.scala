package com.westar.spark.rdd.scala

import com.westar.spark.rdd.人

/**
  * Created by tangweiqun on 2017/8/28.
  */
abstract class 笑脸 {
  def 笑脸模版() = {
    笑前()

    笑()

    笑后()
  }

  def 笑前()

  def 笑()

  def 笑后()
}

object 门面 extends 笑脸 {
  override def 笑前(): Unit = {
    println("刷牙")
  }

  override def 笑(): Unit = {
    println("男士要求站如松 ，刚毅洒脱；女士则应秀雅优美，亭亭玉立")
  }

  override def 笑后(): Unit = {
    println("不能捅刀子")
  }

  def main(args: Array[String]): Unit = {
    笑脸模版()

    val 一个人 = new 人()
    一个人.设置名字("小王")
    println(一个人.toString)
  }
}