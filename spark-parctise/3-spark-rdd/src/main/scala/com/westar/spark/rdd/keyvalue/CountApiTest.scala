package com.westar.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

object CountApiTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CountApiTest")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    //
    val numberRDD = sc.parallelize(1 to 10000 , 200)
    //RDD a内容 union 5次，其中有50000个元素
    val numbersRDD = numberRDD ++ numberRDD ++ numberRDD ++ numberRDD ++ numberRDD
    numbersRDD.count()

    //第一个参数是超时时间
    //第二个参数是期望达到近似估计的准确度
    // 如果你不断用0.9来调用countApprox，则我们期望90%的结果数据是正确的count值
    //如果count统计在超时时间内执行完，则不会近视估值，而是取正确的值
    //如果count统计在超时时间内没有执行完，则根据执行完的task的返回值和准确度进行近似估值
    val resultCount = numbersRDD.countApprox(200, 0.9)
    resultCount.initialValue.mean
    resultCount.initialValue.low
    resultCount.initialValue.high
    resultCount.initialValue.confidence

    resultCount.getFinalValue().mean

    numberRDD.countByValue()

    val resultCountValue = numbersRDD.countByValueApprox(200, 0.9)
    resultCountValue.initialValue(1).mean

    //结果是9760，不传参数，默认是0.05
    numbersRDD.countApproxDistinct()
    //结果是9760
    numbersRDD.countApproxDistinct(0.05)
    //8224
    numbersRDD.countApproxDistinct(0.1)
    //10000  参数越小值越精确
    numbersRDD.countApproxDistinct(0.006)

    val pair = sc.parallelize((1 to 10000).zipWithIndex)

    pair.collect()

    val pairFive = pair ++ pair ++ pair ++ pair ++ pair

    pairFive.countByKey()

    pairFive.countByKeyApprox(10, 0.95)

    //用HyperLogLogPlus来实现的
    //也是调用combineByKey来实现的
    //val createCombiner = (v: V) => {
    //  val hll = new HyperLogLogPlus(p, sp)
    //  hll.offer(v)
    //  hll
    //}
    //val mergeValue = (hll: HyperLogLogPlus, v: V) => {
    //  hll.offer(v)
    //  hll
    //}
    //val mergeCombiner = (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
    //  h1.addAll(h2)
    //  h1
    //}
    pairFive.countApproxDistinctByKey(0.1).collect().size

    pairFive.collectAsMap()
    pairFive.lookup(5)
  }

}
