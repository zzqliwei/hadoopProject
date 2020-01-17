package com.westar.dataset.datasource

import com.westar.dataset.Utils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 *  //读取text文件，返回DataFrame text
 *
 *  //读取text文件，返回Dataset textFile
 */
object TextFileTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TextFileTest")
      .master("local")
      .getOrCreate()

    //1: 将json文件数据转化成text文件数据
    val df = spark.read.json(s"${Utils.BASE_PATH}/people.json")
    df.write.mode(SaveMode.Overwrite).text(s"${Utils.BASE_PATH}/text") //报错
    df.select("age").write.mode(SaveMode.Overwrite).text(s"${Utils.BASE_PATH}/text") //报错

    //compression
    //`none`, `bzip2`, `gzip`
    //todo 支持哪些压缩格式呢？
    df.select("name").write.mode(SaveMode.Overwrite).option("compression", "bzip2").text(s"${Utils.BASE_PATH}/text")

    //读取text文件，返回DataFrame
    val textDF = spark.read.text(s"${Utils.BASE_PATH}/text")
    textDF.show()
    //读取text文件，返回Dataset
    val textDs = spark.read.textFile(s"${Utils.BASE_PATH}/text")
    textDs.show()

    spark.stop()
    
  }
}