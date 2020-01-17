package com.westar.dataset.datasource

import com.westar.dataset.Utils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * //3: parquet schema merge
 * //全局设置spark.sql.parquet.mergeSchema = true
 *
 */
object ParquetFileTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ParquetFileTest")
      .getOrCreate()

    //1: 将json文件数据转化成parquet文件数据
    val df = spark.read.json(s"${Utils.BASE_PATH}/people.json")
    df.show()

    //gzip、lzo、snappy
    df.write.mode(SaveMode.Overwrite).option("compression","snappy").parquet(s"${Utils.BASE_PATH}/parquet")
    //2: 读取parquet文件
    val parquetDF = spark.read.parquet(s"${Utils.BASE_PATH}/parquet")
    parquetDF.show()

    //3: parquet schema merge
    //全局设置spark.sql.parquet.mergeSchema = true
    df.toDF("age", "first_name").write.parquet(s"${Utils.BASE_PATH}/parquet_schema_change")
    val changedDF = spark.read.parquet(s"${Utils.BASE_PATH}/parquet_schema_change")
    changedDF.show()

    val schemaMergeDF = spark.read.option("mergeSchema", "true").parquet(s"${Utils.BASE_PATH}/parquet",
      s"${Utils.BASE_PATH}/parquet_schema_change")
    schemaMergeDF.show()

    spark.stop()

  }

}
