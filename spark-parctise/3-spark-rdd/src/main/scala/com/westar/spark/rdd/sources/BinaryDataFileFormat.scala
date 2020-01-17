package com.westar.spark.rdd.sources

import java.io.FileOutputStream

import org.apache.spark.{SparkConf, SparkContext}

object BinaryDataFileFormat {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("BinaryDataFileFormat")
    conf.setMaster("local[*]")
    val sc = new SparkContext()

    val wholeTextFiles = sc.wholeTextFiles("hdfs://hadoop0:9000/users/hadoop/text/")
    wholeTextFiles.collect()

    val binaryFilesRDD = sc.binaryFiles("hdfs://hadoop0:9000/users/hadoop/text/")
    binaryFilesRDD.collect()

    binaryFilesRDD.map{ case (fileName,stream)=>
      (fileName,new String(stream.toArray()))
    }.collect()


    //可以用于将hdfs上的脚本同步到每一个executor上
    val binaryFilesStreams = binaryFilesRDD.collect()
    val binaryFilesStreamsB = sc.broadcast(binaryFilesStreams)

    val data = sc.parallelize(Seq(2, 3, 5, 2, 1), 2)
    data.foreachPartition(iter =>{
      val allFileStreams = binaryFilesStreamsB.value
      allFileStreams.foreach{ case (fileName,stream)=>
        val inputStream = stream.open()
        val fileOutputStream = new FileOutputStream(s"/local/path/fileName-${fileName}")

        val buf = new Array[Byte](4096)
        var hasData = true

        while (hasData) {
          val r = inputStream.read(buf)
          if (r == -1) hasData = false
          fileOutputStream.write(buf, 0, r)
        }
        inputStream.close()
      }
    })

    val binaryFileData = sc.parallelize[Array[Byte]](List(Array[Byte](2, 3),
      Array[Byte](3, 4), Array[Byte](5, 6)))

    binaryFileData.saveAsTextFile("hdfs://hadoop0:9000/users/hadoop/binary/")
    val binaryRecordsRDD = sc.binaryRecords("hdfs://hadoop0:9000/users/hadoop/binary/part-00002", 2)
    binaryRecordsRDD.collect()
  }

}
