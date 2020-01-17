package com.westar.dataset.datasource

import com.westar.dataset.Utils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object BasicTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BasicTest")
      .master("local")
      .getOrCreate()
    //最基本的读取(load)和保存(write)操作，操作的文件的数据格式默认是parquet
    val sessionDF = spark.read.load(s"${Utils.BASE_PATH}/trackerSession")
    sessionDF.show()

    sessionDF.select("ip","cookie")
      .write.save(s"${Utils.BASE_PATH}/trackerSession_ip_cookie")

    //可以读取多个文件目录下的数据文件
    val multiSessionDF = spark.read.load(s"${Utils.BASE_PATH}/trackerSession",
      s"${Utils.BASE_PATH}/trackerSession_ip_cookie")
    multiSessionDF.show()

    //读取的时候指定schema
    val schema = StructType(StructField("ip", StringType)::Nil)
    val specSessionDF = spark.read.schema(schema).load(s"${Utils.BASE_PATH}/trackerSession")
    specSessionDF.show()

    //指定数据源数据格式
    //读取json文件, 且将读取出来的数据保存为parquet文件
    val deviceInfoDF = spark.read.json(s"${Utils.BASE_PATH}/IoT_device_info.json")
    deviceInfoDF.write.orc(s"${Utils.BASE_PATH}/iot2")

    //option传递参数，改变读写数据源的行为
    spark.read.option("mergeSchema", "true").parquet(s"${Utils.BASE_PATH}/trackerSession")
    deviceInfoDF.write.option("compression", "snappy").parquet(s"${Utils.BASE_PATH}/iot2_parquet")

    val optsMap = Map("mergeSchema" -> "mergeSchema")
    spark.read.options(optsMap).parquet("")

    //SaveMode
    //SaveMode.ErrorIfExists(对应着字符串"error"):表示如果目标文件目录中数据已经存在了，则抛异常(这个是默认的配置)
    //SaveMode.Append(对应着字符串"append"):表示如果目标文件目录中数据已经存在了,则将数据追加到目标文件中
    //SaveMode.Overwrite(对应着字符串"overwrite"):表示如果目标文件目录中数据已经存在了，则用需要保存的数据覆盖掉已经存在的数据
    //SaveMode.Ignore(对应着字符串为:"ignore"):表示如果目标文件目录中数据已经存在了,则不做任何操作

    deviceInfoDF.write.option("compression", "snappy").mode(SaveMode.Ignore).parquet(s"${Utils.BASE_PATH}/iot/iot2_parquet")
    spark.read.parquet(s"${Utils.BASE_PATH}/iot/iot2_parquet").show()
    deviceInfoDF.write.option("compression", "snappy").mode("ignore").parquet(s"${Utils.BASE_PATH}/iot/iot2_parquet")

    spark.stop()


  }

}
