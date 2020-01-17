package com.westar.spark.rdd.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/24.
  */
object MapApiTest {

  def main(args: Array[String]): Unit = {

    //闭包是一个函数
    val addTwoFun = (i: Int) => i + 2
    addTwoFun(2)

    //应用到函数外面定义的变量，定义这个函数的过程是将这个自由变量捕获而构成一个封闭的函数
    //从而构成一个闭包
    var factor = 2
    val addFactorFun = (i: Int) => i + factor
    addFactorFun(2)

    factor = 3
    addFactorFun(2)

    //addFactorFun作为一个对象，是可以序列化的
    addFactorFun.getClass //Class[_ <: Int => Int] = class $anonfun$1
    addFactorFun.isInstanceOf[Function1[Int, Int]] // true
    addFactorFun.isInstanceOf[Serializable] //true


    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)

    val listRDD = sc.parallelize[Int](Seq(1, 2, 3, 3), 2)

    // 因为 函数 x => x + 1是可以序列化的，所以这里没有问题
    val mapRDD = listRDD.map(x => x + 1)
    mapRDD.collect()

    //因为函数addTwoFun依赖到了类AdderWithField中的成员变量n，
    //进而这个函数addTwoFun会依赖类AdderWithField
    //然而类AdderWithField是不能被序列化的，所以这个闭包是不能被序列化的
    val addMapRDDError = listRDD.map(new AdderWithField().addTwoFun)

    //这里的函数addTwoFun是不会依赖Adder中的任何成员变量
    //所以相对于addTwoFun来说，变量new Adder()在闭包中是无效的，
    //所以会被spark清除掉这个无用变量
    val adder = new Adder()
    val addMapRDD = listRDD.map(adder.addTwoFun)
    addMapRDD.collect()

  }

}

class AdderWithField {
  val n = 2
  def addTwoFun = (i: Int) => i + n
}

class Adder {
  def addTwoFun = (i: Int) => i + 2
}
