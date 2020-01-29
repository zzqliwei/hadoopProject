package com.westar.hbase.spark.usage

import com.westar.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HBaseCounter {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseCounter")
    sparkConf.setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    val hbaseConf = HBaseConfiguration.create()

    val hbaseContext = new HBaseContext(sparkContext,hbaseConf)

    val scan = new Scan()
    scan.setCaching(100)

    val keyOnlyFilter = new KeyOnlyFilter()
    scan.setFilter(keyOnlyFilter)

    val data = hbaseContext.hbaseRDD(TableName.valueOf("user"), scan)

    print(s"nomal data.count = ${data.count()}" )

    //reduceCount(data)

    //aggregateCount(data)
  }

  private def aggregateCount(data: RDD[(ImmutableBytesWritable, Result)]) = {
    val aggregateCounter =
      data.aggregate(0)((v1: Int, v2: (ImmutableBytesWritable, Result)) => v1 + 1,
        (v1: Int, v2: Int) => v1 + v2)
    println(s"aggregateCount = ${aggregateCounter}")
  }

  private def reduceCount(data: RDD[(ImmutableBytesWritable, Result)]) = {
    val reduceCounter = data.mapPartitions(it => {
      var count = 0
      it.foreach(_ => count = count + 1)
      Iterator(count)
    }).reduce(_ + _)

    println(s"reduceCount = ${reduceCounter}")
  }

}
