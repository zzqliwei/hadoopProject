package com.westar.spark.rdd.partition

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * mapValues 不会修改分区器
 * map 会修改分区器
 */
object PageRankTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DomainNamePartitioner")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val links =
      sc.parallelize[(String, Seq[String])](Seq(
        ("baidu.com", Seq("jd.com", "ali.com")),
        ("ali.com", Seq("test.com")),
        ("jd.com", Seq("baidu.com"))
      )).partitionBy(new HashPartitioner(3)).persist()

    var ranks = links.mapValues(v => 1.0)
    for(i <- 1 until  10){
      val contributions = links.join(ranks).flatMap{
        case (pageId,(links,rank)) =>{
          links.map(dest=>(dest,rank/links.size))
        }
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }
    ranks.foreach(println(_))


    ranks.saveAsTextFile("hdfs://hadoop0:9000/users/hadoop-twq/ranks")

    val testRDD = sc.parallelize[(String, Int)](Seq(("baidu", 2),
      ("ali", 2), ("jd", 3), ("hhh", 4), ("hhh", 2),
      ("ali", 2), ("jd", 2), ("mmss", 5))).partitionBy(new HashPartitioner(3)).persist()

    val reduceByKeyRDD = testRDD.reduceByKey(new HashPartitioner(3), (x, y) => x + y)

    val joined = testRDD.join(reduceByKeyRDD)
    joined.collect()

    val mapValuesRDD = testRDD.mapValues(x => x + 1)
    mapValuesRDD.collect()
    mapValuesRDD.partitioner

    val nonShuffleJoined = mapValuesRDD.join(reduceByKeyRDD)

    val mapRDD = testRDD.map { case (key, value) => (key, value + 1) }
    mapRDD.collect()
    mapRDD.partitioner //None
    val shouldShuffleJoin = mapRDD.join(reduceByKeyRDD)

  }

}
