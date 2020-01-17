package com.westar.spark.rdd.sources

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.databricks.spark.avro._

/**
 * import spark.implicits._
 * com.databricks.spark.avro._
 */
object ParquetAvroApiTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }
    val spark = SparkSession.builder()
      .appName("ParquetAvroApiTest")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val personDf = spark.sparkContext.parallelize(Seq(Person("jeffy", 30), Person("tomy", 23)), 1).toDF()
    //avro
    personDf.write.mode(SaveMode.Overwrite).avro("hdfs://master:9999/users/hadoop-twq/avro")
    val avroPersonDf = spark.read.avro("hdfs://master:9999/users/hadoop-twq/avro")
    avroPersonDf.show()

    //parquet
    personDf.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9999/users/hadoop-twq/parquet")
    val parquetPersonDF = spark.read.parquet("hdfs://master:9999/users/hadoop-twq/parquet")
    parquetPersonDF.show()

    //json
    personDf.write.mode(SaveMode.Overwrite).json("hdfs://master:9999/users/hadoop-twq/json")
    val jsonPersonDf = spark.read.json("hdfs://master:9999/users/hadoop-twq/json")
    jsonPersonDf.show()

  }

}
