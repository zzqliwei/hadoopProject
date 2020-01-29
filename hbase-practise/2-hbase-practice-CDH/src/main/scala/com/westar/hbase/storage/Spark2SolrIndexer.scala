package com.westar.hbase.storage

import java.util.UUID

import com.westar.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object Spark2SolrIndexer {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val spark = SparkSession
      .builder()
      .appName("Spark2SolrIndexer")
      .master("local[*]")
      .getOrCreate()

    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConf)

    val scan = new Scan()
    scan.setCaching(10)
    scan.setBatch(30)

    val sensorRDD = hbaseContext.hbaseRDD(TableName.valueOf("sensor"), scan)
    import scala.collection.JavaConversions._
    val sensorRowRDD = sensorRDD.mapPartitions { case iterator =>
      val event = new Event()
      iterator.flatMap{case(_,result) =>
        val cells = result.listCells()
        cells.map {cell =>
          val finalEvent = new CellEventConvertor().cellToEvent(cell, event)
          Row(UUID.randomUUID().toString, CellUtil.cloneRow(cell), finalEvent.getEventType.toString, finalEvent.getPartName.toString)
        }
      }
    }

    val schema =
      StructType(
        StructField("id", StringType, false) ::
          StructField("rowkey", BinaryType, true) ::
          StructField("eventType", StringType, false) ::
          StructField("partName", StringType, false) :: Nil)

    val sensorDF = spark.createDataFrame(sensorRowRDD, schema)

    import com.lucidworks.spark.util.SolrDataFrameImplicits._

    val options = Map(
      "zkhost" -> "slave3:2181,slave1:2181,slave2:2181/solr"
    )

    sensorDF.write.options(options).mode(SaveMode.Overwrite).solr("sensor")

  }

}
