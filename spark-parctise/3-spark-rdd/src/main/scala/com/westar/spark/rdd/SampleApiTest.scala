package com.westar.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 采样
 * sample
 * randomSplit
 * sampleByKey
 * sampleByKeyExact
 */
object SampleApiTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SampleApiTest")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val listRDD = sc.parallelize[Int](Seq(1, 2, 3, 3), 2)


    //第一个参数为withReplacement
    //如果withReplacement=true的话表示有放回的抽样，采用泊松抽样算法实现
    //如果withReplacement=false的话表示无放回的抽样，采用伯努利抽样算法实现

    //第二个参数为：fraction，表示每一个元素被抽取为样本的概率，并不是表示需要抽取的数据量的因子
    //比如从100个数据中抽样，fraction=0.2，并不是表示需要抽取100 * 0.2 = 20个数据，
    //而是表示100个元素的被抽取为样本概率为0.2;样本的大小并不是固定的，而是服从二项分布
    //当withReplacement=true的时候fraction>=0
    //当withReplacement=false的时候 0 < fraction < 1

    //第三个参数为：reed表示生成随机数的种子，即根据这个reed为rdd的每一个分区生成一个随机种子
    val sampleRDD = listRDD.sample(false,0.5,100)
    sampleRDD.glom().collect()

    //按照权重对RDD进行随机抽样切分，有几个权重就切分成几个RDD
    //随机抽样采用伯努利抽样算法实现
    val splitRDD =  listRDD.randomSplit(Array(0.8,0.2))
    splitRDD.size
    splitRDD(0).glom().collect()
    splitRDD(1).glom().collect()

    //随机抽样指定数量的样本数据
    listRDD.takeSample(false,10,100)

    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6), (5, 6)), 4)

    //分层采样
    val fractions = Map(1 -> 0.3, 3 -> 0.6, 5 -> 0.3)
    val sampleByKeyRDD =pairRDD.sampleByKey(true,fractions)
    sampleByKeyRDD.glom().collect()

    val sampleByKeyExac = pairRDD.sampleByKeyExact(false,fractions)
    sampleByKeyExac.glom().collect()



  }

}
