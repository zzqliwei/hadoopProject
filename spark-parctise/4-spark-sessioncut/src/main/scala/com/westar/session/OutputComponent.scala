package com.westar.session

import com.westar.spark.session.{TrackerLog, TrackerSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait OutputComponent {
  def writeOutputData(sc:SparkContext, baseOutputPath:String,
                      parsedLogRDD:RDD[TrackerLog], cookieLabeledSessionRDD:RDD[TrackerSession]): Unit ={
    deleteIfExists(sc, baseOutputPath)
  }

  private def deleteIfExists(sc: SparkContext, baseOutputPath: String): Unit = {
    val path = new Path(baseOutputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
  }
}

object OutputComponent{
  def fromOutPutFileType(fileType: String) ={
    if (fileType.equals("parquet")) {
      new ParquetFileOutput
    } else {
      new TextFileOutput
    }
  }
}

class ParquetFileOutput extends OutputComponent{
  override def writeOutputData(sc: SparkContext, baseOutputPath: String, parsedLogRDD: RDD[TrackerLog], cookieLabeledSessionRDD: RDD[TrackerSession]): Unit = {
    super.writeOutputData(sc,baseOutputPath,parsedLogRDD,cookieLabeledSessionRDD)
    // 6. 保存结果数据
    // 6.1 保存TrackerLog --> parsedLogRDD
    val trackerLogOutputPath = s"${baseOutputPath}/trackerLog"

    AvroWriteSupport.setSchema(sc.hadoopConfiguration,TrackerLog.SCHEMA$)
    parsedLogRDD.map((null,_)).saveAsNewAPIHadoopFile(trackerLogOutputPath,
      classOf[Void],classOf[TrackerLog],classOf[AvroParquetOutputFormat[TrackerLog]])

    // 6.2 保存TrackerSession --> cookieLabeledSessionRDD
    val trackerSessionOutputPath = s"${baseOutputPath}/trackerSession"
    AvroWriteSupport.setSchema(sc.hadoopConfiguration,TrackerSession.SCHEMA$)
    cookieLabeledSessionRDD.map((null,_)).saveAsNewAPIHadoopFile(trackerSessionOutputPath,
      classOf[Void],classOf[TrackerSession],classOf[AvroParquetOutputFormat[TrackerSession]])

  }
}

class TextFileOutput extends  OutputComponent{
  override def writeOutputData(sc: SparkContext, baseOutputPath: String, parsedLogRDD: RDD[TrackerLog], cookieLabeledSessionRDD: RDD[TrackerSession]): Unit = {
    super.writeOutputData(sc,baseOutputPath,parsedLogRDD,cookieLabeledSessionRDD)
    // 6. 保存结果数据
    // 6.1 保存TrackerLog --> parsedLogRDD
    val trackerLogOutputPath = s"${baseOutputPath}/trackerLog"
    parsedLogRDD.saveAsTextFile(trackerLogOutputPath)

    // 6.2 保存TrackerSession --> cookieLabeledSessionRDD
    val trackerSessionOutputPath = s"${baseOutputPath}/trackerSession"

    cookieLabeledSessionRDD.saveAsTextFile(trackerSessionOutputPath)
  }
}
