package com.westar.topn

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object GroupTopN {

  def main(args: Array[String]): Unit = {
    val n = 3
    runningSparkJob(createSparkContext,sparkContext =>{

      val lines = sparkContext.textFile("spark-parctise/5-spark-rdd-practice/data/grouptopn/test.txt")
      val kvRDD =lines.map(line =>{
        val temp = line.split(",")
        (temp(0), temp(1).toInt)
      })

      val group = kvRDD.groupByKey() // 将相同的key(课程名称)以及对应的所有的分数放到同一个分区里
      val groupTopN = group.map{ case (key,iter) =>
        //  1、有可能出现OOM (Out Of Memory)内存溢出
        //  2、这个key所在的这个task会非常非常的慢
        // -- 数据倾斜
        (key,iter.toList.sorted(Ordering.Int.reverse).take(n))
      }

      FileSystem.get(sparkContext.hadoopConfiguration).delete(new Path("spark-parctise/5-spark-rdd-practice/result/grouptopn"), true)
      groupTopN.saveAsTextFile("spark-parctise/5-spark-rdd-practice/result/grouptopn")
    })
  }


  /**
   * 创建一个SparkContext
   */
  def createSparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("mock-topn")
    SparkContext.getOrCreate(conf)
  }

  /**
   * 运行spark程序
   *
   * @param createSparkContext
   * @param operator
   * @param closeSparkContext
   */
  def runningSparkJob(createSparkContext: => SparkContext, operator: SparkContext => Unit,
                      closeSparkContext: Boolean = false): Unit = {
    // 创建上下文
    val sc = createSparkContext
    // 执行并在执行后关闭上下文
    try operator(sc)
    finally if (closeSparkContext) sc.stop()
  }
}
