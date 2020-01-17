package com.westar.topn

import java.util.concurrent.ThreadLocalRandom

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object MockTopNData {
  val charts = "qazwsxedcrfvtgbyhnujmikolp0123456789"
    .toCharArray()
    .zipWithIndex // 转换为二元组
    .filter(_._2 % 5 == 0) // 减少数据量
    .map(_._1)

  val chartsLength = charts.length
  val random = ThreadLocalRandom.current()

  // 产生url的原始数据
  val protocols = Array("https://","http://")
  val domains = Array(
    "www.baidu.com",
    "www.51cto.com",
    "www.qq.com",
    "www.yahoo.com",
    "fanyi.baidu.com",
    "pic.baidu.com",
    "ip.taobao.com",
    "www.taobao.com",
    "www.cnblogs.com",
    "www.csdn.com",
    "www.hao123.com",
    "www.apache.org",
    "hadoop.apache.org",
    "maven.apache.org",
    "hbase.apache.org",
    "hive.apache.org",
    "spark.apache.org"
  )
  val protocolSize = protocols.length
  val domainSize = domains.length

  /**
   * 产生单词的函数
   */
  def generateWord = (chartSize: Int) =>{
    // 随机产生一个chartSize个字符长度的字符
    (0 until chartSize).map( v => charts(random.nextInt(chartsLength))).mkString("")
  }
  /**
   * 产生一行数据(字符串)的函数
   */
  val generateLine = (words: Int) =>{
    // 随机产生一个由words个单词构成的字符串
    (0 until words).map( v => generateWord(random.nextInt(2,5))).mkString(" ")
  }
  /**
   * 随机一个url数据
   */
  val generateUrl = () =>{
    // 获取协议
    val protocol = protocols(random.nextInt(protocolSize))
    // 获取domain
    val domain = domains(random.nextInt(domainSize))
    // 获取请求资源
    val endpointSize = random.nextInt(1,5)

    val endpoint = (0 until endpointSize).map(index =>{
      // 随机一个单词并返回
      generateLine(random.nextInt(1, 3))
    }).mkString("/")

    // 获取请求参数
    val parameterSize = random.nextInt(1,4)
    val parameter = (0 until parameterSize).map(index =>{
      // 随机一个参数的key
      val key = generateWord(random.nextInt(1,3))
      val value = generateWord(random.nextInt(1,3))
      // 构建参数并返回
      s"${key}=${value}"
    }).mkString("&")
    // 构建url并返回
    s"${protocol}${domain}/${endpoint}?${parameter}"
  }

  def main(args: Array[String]): Unit = {
    val n=10000
    val path = s"spark-parctise/5-spark-rdd-practice/data/topn/${n}"
    val urlPath = s"spark-parctise/5-spark-rdd-practice/data/url/${n}"
    // 创建一个SparkContext
    val createSparkContext = {
      val conf = new SparkConf()
      conf.setAppName("MockTopNData")
      conf.setMaster("local[*]")

      SparkContext.getOrCreate(conf)
    }

    // 产生单词数据
    mockData(createSparkContext,sc => {
      val data =  (0 until n ).flatMap(index =>{
        // 随机一个单词数量
        val words = random.nextInt(1,5)
        // 产生一行数据
        val line = generateLine(words)
        // 返回结果, 并重复多行
        val times = random.nextInt(1,20)
        (0 until times).map(i => line)
      })

      // 构建RDD
      val rdd = sc.parallelize(data)
      // rdd数据保存
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(path),true)
      rdd.repartition(1).saveAsTextFile(path)
    })

    // 产生url数据
    mockData(createSparkContext,sc =>{
      val data = (0 until n).map(index =>{
        // 随机一个url
        val url =generateUrl()
        url
      })
      // 构建RDD
      val rdd = sc.parallelize(data)

      // rdd数据保存
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(urlPath), true)
      rdd.repartition(1).saveAsTextFile(urlPath)
    },true)
  }


  def mockData(createSparkContext: => SparkContext,
               operator: SparkContext => Unit,
               closeSparkContext: Boolean=false): Unit ={
    // 创建上下文
    val sc = createSparkContext
    // 执行并在执行后关闭上下文
    try operator(sc)
    finally if(closeSparkContext) sc.stop()
  }


}
