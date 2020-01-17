package com.westar.spark.rdd.partition

import java.net.URL

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * 自定义的分区器
 * @param numParts
 */
class DomainNamePartitioner(val numParts:Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = new URL(key.toString).getHost
    val code = domain.hashCode % numParts
    if(code < 0){
      code + numParts
    }else{
      code
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case dnp:DomainNamePartitioner =>{
      dnp.numParts == numParts
    }
    case _ => false
  }

  override def hashCode(): Int = numParts
}

object DomainNamePartitioner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DomainNamePartitioner")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val urlRDD = sc.makeRDD(Seq(("http://baidu.com/test", 2),
      ("http://baidu.com/index", 2), ("http://ali.com", 3), ("http://baidu.com/tmmmm", 4),
      ("http://baidu.com/test", 4)))

    val hashPartitionedRDD = urlRDD.partitionBy(new HashPartitioner(2))
    hashPartitionedRDD.glom().collect()

    //使用spark-shell --jar的方式将这个partitioner所在的jar包引进去，然后测试下面的代码
    // spark-shell --master spark://master:7077 --jars spark-rdd-1.0-SNAPSHOT.jar
    val partitionedRDD = urlRDD.partitionBy(new DomainNamePartitioner(2) )
    partitionedRDD.glom().collect()
  }

}
