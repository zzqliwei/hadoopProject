package com.westar.hbase.spark.usage

import com.westar.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object IngestionData2HbaseWithPutOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IngestionData2HbaseWithPutOpt")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    val textFile = sparkContext.textFile("hdfs://master:9999/user/hadoop-twq//hbase-course/spark/data.txt")

    val pairs = textFile.map { line =>
      val index = line.indexOf("|")
      (line.substring(0, index), line.substring(index + 1))
    }
    val createCombiner = (s:String) =>{
      val buffer = new ArrayBuffer[String]()
      buffer += s
      buffer
    }

    val mergeValue = (buffer:ArrayBuffer[String],value:String) =>{
      buffer +=value
      buffer
    }

    val mergeCombiner = (buffer1:ArrayBuffer[String],buffer2:ArrayBuffer[String]) =>{
      buffer1 ++=buffer2
      buffer1
    }

    val keyValues = pairs.combineByKey(createCombiner,mergeValue,mergeCombiner)

    val keyValuePuts = keyValues.map{case(key,buffer) =>
        val put = new Put(Bytes.toBytes(key))
        buffer.foreach{colAndValue =>
          val indexDelimiter = colAndValue.indexOf("|")
          val cq = colAndValue.substring(0, indexDelimiter)
          val value = colAndValue.substring(indexDelimiter + 1)
          put.addColumn(Bytes.toBytes("segment"), Bytes.toBytes(cq), Bytes.toBytes(value))
        }
        put
    }

    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sparkContext, hbaseConf)

    hbaseContext.foreachPartition(keyValuePuts,(it:Iterator[Put],conn:Connection)=>{
      val mutator = conn.getBufferedMutator(TableName.valueOf("user"))
      it.foreach{put=>
        mutator.mutate(put)
      }
      mutator.flush()
      mutator.close()
    })

    sparkContext.stop()

  }

}
