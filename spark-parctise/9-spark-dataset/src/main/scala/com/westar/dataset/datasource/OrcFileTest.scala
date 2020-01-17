package com.westar.dataset.datasource

import com.westar.dataset.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object OrcFileTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }

    val spark = SparkSession
      .builder()
      .appName("OrcFileTest")
      .config(conf)
      .getOrCreate()

    //1: 将json文件数据转化成orc文件数据
    val df = spark.read.json(s"${Utils.BASE_PATH}/people.json")
    df.show()

    df.write.option("compression", "snappy").orc(s"${Utils.BASE_PATH}/orc")

    val orcFileDF = spark.read.orc(s"${Utils.BASE_PATH}/orc")
    orcFileDF.show()

    spark.stop()
  }

}
