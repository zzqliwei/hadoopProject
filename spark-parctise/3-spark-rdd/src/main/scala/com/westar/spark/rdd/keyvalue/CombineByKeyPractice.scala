package com.westar.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyPractice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CombineByKeyPractice")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pairStrRDD = sc.parallelize[(String, Double)](Seq(("coffee", 0.6),
      ("coffee", -0.1), ("panda", -0.3), ("coffee", 0.1)), 2)

    def createCombiner = (label: Double) => new BinaryLabelCounter(0L, 0L) += label

    def mergeValue = (c: BinaryLabelCounter, label: Double) => c += label

    def mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2

    var testCombineByKeyRDD = pairStrRDD.combineByKey(createCombiner,mergeValue,mergeCombiners)
    testCombineByKeyRDD.collect()
  }

}

class BinaryLabelCounter(var numPositives: Long = 0L,
                         var numNegatives: Long = 0L ) extends Serializable{
  def +=(lable:Double): BinaryLabelCounter = {
      if(lable > 0){
        numPositives +=1L
      }else{
        numNegatives +=1L
      }
    this
  }
  def +=(other: BinaryLabelCounter): BinaryLabelCounter = {
    numPositives += other.numPositives
    numNegatives += other.numNegatives
    this
  }
}
