package com.westar.hbase.spark.streaming

import com.westar.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * create 'test_streaming','f'
  */
object StreamingHbaseTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "StreamingHbaseTest")
    val config = HBaseConfiguration.create()

    val hbaseContext = new HBaseContext(sc,config)
    val ssc = new StreamingContext(sc, Seconds(3))

    val rdd1 = sc.parallelize(Seq((Bytes.toBytes("row-1"),
      Array((Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("value1"))))))

    val rdd2 = sc.parallelize(Seq((Bytes.toBytes("row-2"),
      Array((Bytes.toBytes("f"), Bytes.toBytes("e"), Bytes.toBytes("value3"))))))

    val queue = mutable.Queue[RDD[(Array[Byte], Array[(Array[Byte],
      Array[Byte], Array[Byte])])]]()

    queue += rdd1
    queue += rdd2

    val dStream = ssc.queueStream(queue)

    import com.westar.hbase.spark.HBaseDStreamFunctions._

    dStream.hbaseBulkPut(
      hbaseContext,
      TableName.valueOf("test_streaming"),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })

    ssc.start()
    ssc.awaitTermination()

  }

}
